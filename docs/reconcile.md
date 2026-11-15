# Cross-source reconciliation v1

对两个 `stream` 状态库做跨源事件对账：各自取一致只读快照，仅纳入已接受事件，
按双目标（先最大化配对数，再最小化绝对时间差总和）求最优配对并安全发布报告。
原单流与流式契约见 [contract.md](contract.md) 与 [stream.md](stream.md)，三者互不影响。

## 命令

```sh
watermark-windows reconcile --a run/a.db --b run/b.db --tolerance 5 --output run/reconcile.json
```

`--a/--b` 为两个 `stream init` 建立的状态库（允许同一个库双侧对账）；`--tolerance`
为非负整数（0..10^12秒，与event_time同界）；`--output` 为报告路径。
成功退出0并把报告JSON同时打印到标准输出；源库或参数非法退出2并保留旧报告。

## 快照与来源

每个源库以 `mode=ro` 只读打开，在单个读事务内完成全部读取——读到的是事务开始时刻的
一致快照；读取期间并发 `stream apply` 提交不会造成撕裂（结果要么全旧要么全新，
取决于快照先于还是后于对方提交）。reconcile 不写源库、不建临时表、不改journal。

仅纳入 `contributions` 对应的已接受事件（与 `seen` 按id连接）：迟到事件
（`late_events`）、内容一致的重复ID、offset重传都不产生贡献，一律排除。
每源将已接受事件按 `(event_time, id)` 排序编号为规范序列（id源内唯一，故序全序确定），
下标 0..n-1 即报告中的 `a_index/b_index`。

## 配对规则与双目标

事件 `a[i]` 与 `b[j]` 可配对当且仅当 `value` 相等且 `|event_time差| <= tolerance`。
配对集合中每条事件最多出现一次，且配对下标双边严格递增（保序对齐）。
目标：先最大化配对数 `pair_count`，再最小化绝对时间差总和 `total_abs_time_delta`。
并列最优由确定规则任选（DP在内部工作方向上取最小切分点与最小下标；为满足线性空间，
工作方向取较短侧为DP行，可能与展示方向相反），同输入重跑结果逐字节一致。

## 算法与空间

双目标编码为单整数分数 `pairs*W - total_delta`（`W = min(n,m)*10^12 + 1`，
任意配对集合的时间差总和都小于W，故字典序等价），用 Hirschberg 分治求解：
每次把较长侧对半，正/逆两个方向各算一行DP（行长 `min(n,m)+1`），取使两侧分数之和
最大的切分点递归；配对明细由递归重建，不保存 n×m 大小的表。
除输入与输出外，工作空间为 `O(min(n,m) + log(max(n,m)+1))`（递归深度对数级）。
时间 `O(n*m)`；单侧6000条实测通过（见tests/test_reconcile.py峰值内存实测）。

## 安全发布

报告发布沿用原子替换与符号链接拒绝（同 `stream report`）；输出路径不得覆盖任一源库。
参数非法（tolerance越界/非整数、缺参数）、源库缺失/损坏/非stream库/schema不支持时
一律退出2，旧报告保持不变；源库全程只读，reconcile前后字节不变。

## 规范序列SHA256编码

`sources.a.canonical_sha256` / `sources.b.canonical_sha256` 为各源规范事件序列的SHA256。
编码：按 `(event_time, id)` 排序后，每条事件一行
`{"event_time":<int>,"id":<str>,"value":<int>}`（键按字典序、逗号冒号紧凑分隔、
非ASCII不转义）加换行符 `\n`，连接后取UTF-8字节的SHA256；空序列为空输入的SHA256
（`e3b0c442...`）。同一源库同一提交点重算结果不变，可用于跨源指纹比对。

## 报告字段

- `schema_version`：恒1；`tolerance`：本次使用的容差。
- `objective`：`pair_count` 配对数，`total_abs_time_delta` 配对绝对时间差总和。
- `sources.a/b`：`accepted_events` 规范序列长度，`canonical_sha256` 见上节编码。
- `pairs`：按 `a_index` 升序（`b_index` 同为升序）的配对明细，含双边下标、
  双边事件id、双边event_time、value与 `time_delta`。
- `unmatched_a` / `unmatched_b`：未配对事件，含 `index/id/event_time/value`，按下标升序。
- 配对数加双边未配对数分别等于各源 `accepted_events`。
