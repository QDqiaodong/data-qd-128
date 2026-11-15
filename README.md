# data-qd-128 · 事件时间水位窗口聚合

Arrival-order tumbling windows with explicit late-event policy。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
.venv/bin/watermark-windows --source fixtures/input.json --output run/report.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/watermark_windows/model.py 为业务模型，相关算法模块独立组织；common.py 提供受限JSON及报告发布，
cli.py 衔接数据链路；stream.py 为分区增量流（SQLite状态、整批事务、水位推进与报告重建）；
reconcile.py 为跨源对账（只读快照、Hirschberg线性空间双目标配对）。fixtures 为明确自制样例，代码与样例采用MIT许可。

## 业务与样例结果

样例接受4条总和14，1条迟到丢弃、1条等价重传；明确保留watermark关闭与输入结束关闭区别。

## 分区增量流（stream）

在固定分区上按JSONL批次增量推进窗口，SQLite存状态，可断点恢复；原`--source/--output`用法不变。
契约详见 [流式数据契约](docs/stream.md)。

```sh
.venv/bin/watermark-windows stream init --db run/state.db --partitions p1,p2 --window-seconds 10 --out-of-order-seconds 5
.venv/bin/watermark-windows stream apply --db run/state.db --batch fixtures/stream/batch1.jsonl --output run/stream-report.json
.venv/bin/watermark-windows stream report --db run/state.db --output run/stream-report.json
```

每分区offset从0连续；整批原子提交，缺口/冲突/idle分区新event/坏行（含批尾）整批回滚。
全局水位取活跃分区局部水位最小值，未见事件分区阻塞、全idle不推进、resume不回退。
apply提交后发布报告；提交后发布前中断时用report仅凭数据库恢复，报告与不中断逐字节一致；结束不flush。
上述样例批接受5条总和15，水位15，窗口[0,10)按watermark关闭（count2/sum3），[10,20)与[20,30)保持pending。

## 跨源对账（reconcile）

对两个stream状态库做跨源事件对账；原`--source/--output`与`stream`用法不变。
契约详见 [对账数据契约](docs/reconcile.md)。

```sh
.venv/bin/watermark-windows reconcile --a run/a.db --b run/b.db --tolerance 5 --output run/reconcile.json
```

各源取一致只读快照，仅纳入contributions对应的已接受事件（排除late、重复ID与重传），
按(event_time,id)排序编号。仅value相同且时间差≤tolerance（非负整数）可配对，每条最多一次，
配对下标双边严格递增；先最大化配对数，再最小化绝对时间差总和，并列最优确定可选、重跑可复现。
报告含配对明细、双边未配对事件、目标值与各源规范事件序列SHA256；失败退出2并保留旧报告，
输出不得覆盖源库。不保存n×m表，工作空间O(min(n,m)+log(max(n,m)+1))，配对明细递归重建。
