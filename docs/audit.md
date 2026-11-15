# Internal audit v1

对一个 `stream` 状态库做内部发布审计：汇总查询计划中的每个受保护事件，判定其单笔
value 在发布后是否可识别，并选出损失（cost）最小的抑制方案，使保留查询下每个受保护
事件都持有不可识别证书。报告为内部JSON，含最优方案、证书与规范输入SHA。原单流、
`stream` 与 `reconcile` 契约互不影响，见 [contract.md](contract.md)、
[stream.md](stream.md)、[reconcile.md](reconcile.md)。

## 命令

```sh
watermark-windows audit --db run/state.db --plan plan.json --output run/audit.json
```

`--db` 为 `stream init` 建立的状态库（只读）；`--plan` 为发布计划JSON；`--output` 为
审计报告路径。成功退出0并把报告JSON同时打印到标准输出；任何输入/计算失败退出2并
保留旧报告。

## 快照与来源

状态库以 `mode=ro` 只读打开，在单个读事务内完成全部读取（一致快照，与 reconcile 相同）；
仅纳入 `contributions` 对应的已接受事件——迟到事件、内容一致的重复ID、offset重传都不
产生贡献，一律排除。被 `stream erase` 擦除的事件既无 `seen` 行、贡献也被原子删除、并留有
永久墓碑，因此绝不会出现在公开事件ID全集中。已接受事件按 `(event_time, id)` 排序为规范
序列（id库内唯一，序全序确定），即报告中的公开事件ID全集顺序。audit 不写状态库、
不建临时表、不改journal。

快照同时读取 `erase_epoch`（schema 1为0），写入报告 `erase_epoch`。计算完成后、发布前
重新校验该epoch：变化即退出2、不替换报告、不写标准输出，旧报告保留。校验与
“原子替换报告文件→写标准输出”在同一共享文件锁内完成，擦除提交必须等发布方完成。

## 计划格式（plan JSON）

```json
{
  "queries": [{"query_id": "q1", "cost": 2, "event_ids": ["A", "X"]}],
  "protected_ids": ["A"]
}
```

- 拒绝重复键、非有限数值与未定义字段；文件≤8MB。
- `queries`：至多22个。每个含 `query_id`（非空标识串，查询间唯一）、`cost`
  （正整数，≤10^9）、`event_ids`（数组，内部不重复；允许空数组=零行查询）。
- `protected_ids`：受保护事件id数组，彼此唯一，至多80个。
- 查询与保护引用的每个事件id都必须属于该库的已接受事件集；引用未知事件、迟到事件或
  **已擦除事件**一律失败（退出2）——已擦除事件不再公开，其id也不能出现在计划中。
  状态库已接受事件数超过80同样失败。

## 公开模型与逐事件判据

发布内容：公开事件ID全集、保留查询的成员及sum；被抑制查询的sum不公开。
每笔单事件value视为**无界有理未知数**（不使用任何整数界）。设 A 为保留查询的
查询-事件0/1矩阵，则受保护事件 i 被泄露当且仅当单位向量 e_i 落在 A 的行空间：
此时攻击者持有有理系数 c 使 cA = e_i，用已公开sum推出该值；反之存在有理向量 d
使 Ad = 0 且 d_i = 1——value 可沿 d 任意变动而不改变任何已公开sum，故不可识别。

审计先评估**原计划**（全部查询保留）：逐受保护事件判定 e_i 是否在行空间，
可识别则给出查询系数 c（cA = e_i）及推出值（= c·查询sum，恰等于库中实际值，
内部报告可见），否则给出核证书 d（Ad = 0、d_i = 1）。

注意判据是逐事件的：若只有 e_i+e_j 落在行空间而 e_i、e_j 各自不在，则两事件
各自都持有 d 证书，按计划不抑制（联合和的泄露不在本判据保证范围内）。

## 抑制方案与目标

选抑制集 S 使保留矩阵 B（A 去掉 S 的行）对每个受保护事件都有 Bd = 0、d_i = 1
的证书。目标：先最小化抑制 cost 总和；同价者取**升序 query_id 列表字典序最小**者。
零行查询（空 event_ids）与重复行（秩亏）按同样规则参与；抑制零行/重复行无收益，
只会被目标函数自然排除。

## 算法

对原计划做一次精确RREF（有理数，Gauss-Jordan）得行空间与全部原计划证书。
随后对至多22个查询做深度优先分支定界：节点携带保留行的精确行阶梯形与每个
仍具风险受保护事件的残量，沿搜索树逐行增量复用，不作浮点判秩。全部秩判定在
模素数 p = 2^61-1 下进行：不超过23行的0/1矩阵的任意子式按Hadamard界小于
23^(23/2) < p，故子式在GF(p)上为零当且仅当在Q上为零，模秩与有理秩精确相等。
剪枝：抑制cost超过当前最优即剪；同价时部分抑制列表字典序已不优即剪。
最优保留矩阵最终用有理数RREF重新出具证书（既约 p/q，仅列非零系数），
并对每个受保护事件验证证书存在；同输入重跑报告逐字节一致。

## 规范输入SHA256编码

`input_sha256` 为规范输入的SHA256。编码：先按 `(event_time, id)` 序逐事件一行
`{"event_time":<int>,"id":<str>,"value":<int>}`；再按 query_id 升序逐查询一行
`{"cost":<int>,"event_ids":[<升序id>],"query_id":<str>}`；最后一行
`{"protected_ids":[<升序id>]}`。每行键按字典序、逗号冒号紧凑分隔、非ASCII不转义，
行尾 `\n`，连接后取UTF-8字节的SHA256。同一状态库同一计划重算结果不变。

## 报告字段

- `schema_version`：恒1；`input_sha256`：见上节；`erase_epoch`：快照时状态库的擦除epoch。
- `publication.event_ids`：公开事件ID全集（规范序）。
- `publication.kept_queries`：保留查询（query_id升序），含 `cost`、`event_ids`
  （计划原序）与 `sum`——即可公开部分。
- `suppression.total_cost` / `suppression.suppressed_queries`：最优抑制方案
  （query_id升序，含cost与成员，**不含sum**）。
- `original_plan.per_event`：原计划逐受保护事件。可识别：
  `coefficients`（query_id→既约"p/q"，仅非零）与 `derived_value`；否则
  `kernel_certificate`（事件id→既约"p/q"，仅非零，含自身系数1/1）。
- `kept_plan.per_event`：保留矩阵下逐受保护事件的核证书（Bd=0、d_i=1）。

## 安全发布

报告发布沿用原子替换与符号链接拒绝（同 `stream report`）；输出路径不得覆盖
状态库或计划文件。库缺失/损坏/非stream库/schema不支持、计划非法（结构、界、
重复id、未知或迟到事件引用、超限）、事件数>80或查询数>22时一律退出2，
旧报告保持不变；状态库全程只读，audit前后字节不变。
