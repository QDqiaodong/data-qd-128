# Partitioned incremental stream windows v1

在固定分区集合上维护可恢复的增量事件时间窗口，状态存于SQLite，单操作者批处理。
原单流全量回放契约见 [contract.md](contract.md)，两者互不影响。

## 命令

```sh
watermark-windows stream init --db state.db --partitions p1,p2 \
    --window-seconds 10 --out-of-order-seconds 5
watermark-windows stream apply --db state.db --batch b1.jsonl --output run/report.json
watermark-windows stream report --db state.db --output run/report.json
```

`init`固定分区名集合、窗口宽度(1..86400秒)与迟延(0..86400秒)，全部分区初始为active；
状态文件已存在则拒绝。`apply`原子应用一个JSONL批次并在提交后发布报告。
`report`仅凭数据库重建完整报告并安全发布，不读任何旧批次日志；恢复入口就是它。

## 批次格式（JSONL）

每行一个JSON对象，拒绝重复键、非有限数值与未定义字段；空行跳过；单批≤8MB、≤20000条。
公共字段：`partition`（必须是init固定的分区名）、`offset`（该分区从0连续递增的整数）、
`kind`（event/idle/resume）。event另带原事件字段`id/event_time/value`，约束同单流契约；
idle与resume不得有任何附加字段。

- `offset == 下一期望值`：新记录，按kind处理。
- `offset < 下一期望值`：重传，必须与已提交记录内容完全一致，否则冲突；一致则为无操作。
- `offset > 下一期望值`：缺口，报错。

任一记录非法（缺口、重传冲突、idle分区上的新event、同ID内容冲突、坏行——包括批尾坏行）
则整批回滚，状态保持上一提交点。已idle分区上的新event非法；先resume再发event。
idle/resume重复标记自身合法（幂等）。

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

## 报告字段

`schema_version/window_seconds/out_of_order_seconds`固定参数；`watermark`当前全局水位
（未推进过为null）；`max_event_time`各分区最大事件时间的最大值（无事件为null）；
`closed_windows`按关闭顺序，含start/end/count/sum/到达顺序event_ids/closed_by(恒watermark)；
`pending_windows`按start排序的同结构窗口（无closed_by）；`late_events`按到达顺序，
含原事件字段、所在partition与watermark_at_arrival；`duplicate_events`重复ID计数；
`accepted_count/accepted_sum`全部窗口（含pending）的接受总量；
`partitions`每分区status/next_offset/max_event_time/局部watermark。
