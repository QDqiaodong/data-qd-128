# Partitioned incremental stream windows v1

在固定分区集合上维护可恢复的增量事件时间窗口，状态存于SQLite，单操作者批处理。
原单流全量回放契约见 [contract.md](contract.md)，两者互不影响。

## 命令

```sh
watermark-windows stream init --db state.db --partitions p1,p2 \
    --window-seconds 10 --out-of-order-seconds 5
watermark-windows stream apply --db state.db --batch b1.jsonl --output run/report.json
watermark-windows stream report --db state.db --output run/report.json
watermark-windows stream erase --db state.db --ids erase.json --output run/report.json
```

`init`固定分区名集合、窗口宽度(1..86400秒)与迟延(0..86400秒)，全部分区初始为active；
状态文件已存在则拒绝。`apply`原子应用一个JSONL批次并在提交后发布报告。
`report`仅凭数据库重建完整报告并安全发布，不读任何旧批次日志；恢复入口就是它。
`erase`按事件ID整批逻辑擦除并在提交后发布报告，见[逻辑擦除](#逻辑擦除)。

## 批次格式（JSONL）

每行一个JSON对象，拒绝重复键、非有限数值与未定义字段；空行跳过；单批≤8MB、≤20000条。
公共字段：`partition`（必须是init固定的分区名）、`offset`（该分区从0连续递增的整数）、
`kind`（event/idle/resume）。event另带原事件字段`id/event_time/value`，约束同单流契约；
idle与resume不得有任何附加字段。

- `offset == 下一期望值`：新记录，按kind处理。
- `offset < 下一期望值`：重传，必须与已提交记录内容一致，否则冲突；一致则为无操作。
  即使该记录的事件载荷已被擦除，也仍以规范记录SHA256判定：同摘要无操作、不同则冲突失败。
- `offset > 下一期望值`：缺口，报错。

任一记录非法（缺口、重传冲突、idle分区上的新event、同ID内容冲突、擦除ID的新event、
坏行——包括批尾坏行）则整批回滚，状态保持上一提交点。已idle分区上的新event非法；
先resume再发event。idle/resume重复标记自身合法（幂等）。

## 水位与窗口

窗口为[start,start+width)，start=floor(event_time/width)*width，跨分区全局共享。
分区最大事件时间仅由被接受的新ID推进（重传、重复ID、迟到事件均不推进），
局部水位=最大时间-迟延。全局水位取活跃分区局部水位的最小值：有活跃分区尚未见到
事件则阻塞不推进；全部idle时不推进；resume只可能让候选值变小，全局水位单调不回退。

每条event先按当时（旧）全局水位判迟到：窗口end<=水位则记late并丢弃贡献，随后才更新
状态并关闭所有end<=新水位的窗口。事件ID全局判等（按id/event_time/value内容）：
内容一致的重复ID计入duplicate_events，冲突则整批失败；迟到ID同样占身份参与去重。
已关闭窗口从不重开。结束不flush：没有end_of_input，未关闭窗口保持pending。

## 事务与恢复

窗口贡献、offset推进、关窗在同一SQLite事务中提交；apply在提交成功后才发布报告。
若提交后、发布前中断（如输出路径非法导致发布失败，退出码2但批次已提交），
直接运行`stream report`即可从数据库重新生成完整报告，与不中断时逐字节一致；
重传已提交的批次也不会改变报告。报告发布沿用原子替换与符号链接拒绝；
输出不得覆盖状态库或输入批次。失败一律退出码2且保留旧报告。

apply与erase以排他文件锁（`flock`解析后的库文件路径）串行化同一库的写入；发布报告的
读取方在“校验擦除epoch→原子替换报告文件→写标准输出”期间持共享锁，因此erase提交
不会穿插在该窗口内（路径别名/符号链接也落到同一inode而互相协调）。

## 逻辑擦除

`erase --ids erase.json`，文件为`{"event_ids": ["A", "B"]}`（≤20000个，重复ID折叠；
拒绝未知/未定义字段、非标识串）。整批在一个事务内：

- 删除这些ID在`seen`、`late_events`中的行与`contributions`中的窗口贡献，并从所在窗口
  扣除count与sum（窗口本身保留，即使变为空窗、即使已关闭：闭窗不重开）；
- 对已提交记录，仅清除明文载荷（`records.body`与`records.event_id`置NULL），保留规范
  记录SHA256（`records.digest`），供旧offset重传判定；
- 每个ID写入永久墓碑`tombstones(id, erased_at)`，并递增`erase_epoch`；墓碑与epoch
  （以及上述全部删除）同一次提交，回滚则全部不生效。

id已知且未墓碑则正常擦除；已墓碑的ID再次出现按幂等跳过，不推进epoch；任一ID未知则
整批回滚（epoch、墓碑、窗口均不变）。不要求介质抹除（VACUUM/覆写），不改变配对/抑制算法。

载荷明文的边界：事件id、value与event_time不残留在`seen`/`late_events`/`records`中。
`partitions.max_event_time`、`windows.start`与`meta.watermark`属于聚合账本——按“水位/
offset不回退、闭窗不重开、空窗保留”的契约保留，因此可能仍等于被擦除事件的时间；这是
增量水位语义所需，不属于可擦除载荷。

**防复活**：`seen`删除后墓碑是ID存在的唯一证据，任何分区在**新offset**上携带已擦除ID
一律整批拒绝（exit 2）；已完成窗口不回退、水位与offset不回退——分区`max_event_time`
与全局`watermark`保留历史高水位（仅窗口贡献被扣除），因此后续事件不会被误判为更早。
旧offset重传按规范记录摘要判定：同摘要无操作（其载荷已清空，仍可幂等重放），不同摘要
冲突失败。跨分区同ID的重复记录（即使分属不同分区的不同offset）一并清除明文载荷，
无载荷的普通重传不会复活它。

事务中断安全：erase的任何一步失败（含`ids`文件中途出现未知ID）都会在提交前ROLLBACK，
epoch、墓碑、`seen`、窗口计数与记录载荷全部回到擦除前的状态，可原样重试。

## schema 迁移

schema 1 状态库在首次`apply`/`report`/`erase`（即`connect`）时于单事务内原地迁移到schema 2：
`records`表增加`digest`与`event_id`列（为每行计算规范记录SHA256）、创建`tombstones`、
写入`erase_epoch=0`、更新`schema`元数据。迁移失败（如旧记录体损坏）整事务回滚，
库保持schema 1，命令exit 2。reconcile/audit只读，可读取schema 1与2，schema 1视为
epoch 0、无墓碑。

## 报告字段

`schema_version/window_seconds/out_of_order_seconds`固定参数；`watermark`当前全局水位
（未推进过为null）；`max_event_time`各分区最大事件时间的最大值（无事件为null）；
`erase_epoch`当前擦除epoch（schema 1迁移后从0开始，无擦除为0）；
`erased_event_ids`全部墓碑ID，按升序；`closed_windows`按关闭顺序，
含start/end/count/sum/到达顺序event_ids/closed_by(恒watermark)；
`pending_windows`按start排序的同结构窗口（无closed_by）；`late_events`按到达顺序，
含原事件字段、所在partition与watermark_at_arrival（擦除的late事件不再出现）；
`duplicate_events`重复ID计数；
`accepted_count/accepted_sum`全部窗口（含pending）的接受总量（擦除后扣除对应贡献）；
`partitions`每分区status/next_offset/max_event_time/局部watermark。
`apply/report/erase`都发布同一结构；擦除后`stream report`重建的报告与erase提交时
发布的报告逐字节一致。
