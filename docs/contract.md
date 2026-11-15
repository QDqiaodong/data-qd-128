# Arrival-order event-time windows v1

面向流数据实验人员，用离线到达序列复现实时时间窗口关闭与迟到策略。events数组顺序就是到达顺序，
不能排序后伪装为相同处理；每项id/event_time/value，时间为0..10^12整数秒，value为±10^9整数。
最多20000行，window_seconds=1..86400，out_of_order_seconds=0..86400，flush为布尔。
窗口为[start,start+width)，start=floor(event_time/width)*width。

处理前用当前watermark检查：若窗口end<=watermark则记late并丢弃该聚合贡献。
被接受事件加入窗口后max_event_time更新，watermark=max_event_time-delay单调推进，
关闭所有end<=watermark的窗口；watermark允许负数，不剪成0。同ID完整一致去重，冲突失败，
已迟到的ID也占身份；重复事件不推进watermark或改变统计。已关闭窗口从不重开或补偿。

flush=true在输入结束关闭剩余窗口并标end_of_input，不能把它当真实watermark已推进到无穷；
false保留pending。输出每窗口count/sum/到达顺序ID、关闭原因、late原事件/当时watermark、
当前最大时间与接受总量。输入结束不伪造空窗口；无事件的watermark/max为null。
样例A/C窗口[0,10) count2/sum6，B窗口[10,20) sum3，D窗口[20,30) sum5；
E晚于窗口关闭被丢弃，B重传1次，接受4条总和14。
本契约为单流全量回放，无真实消息中间件、更新流；分区最小水位、故障checkpoint与持久增量状态
见 [stream.md](stream.md) 的分区增量契约，两者共用窗口与迟到语义。
