# P07 KILL 不消耗普通 root/business 检查点

日期：2026-10-07。Query Application 提供只有 parser-admitted KILL 才接受的
control session route；Frontend session 在普通 management root/business 准入之前
调用它。其他 SET/USE/事务语句不走该路由。control连接准入与既有 session token、
跨principal授权、connection generation匹配和取消 first-wins 仍由原 owner处理。

原路径先给 KILL 自己取得 management root/business，因此 ordinary capacity 满时
可能在真正的取消请求之前拒绝。新的同步控制调用不等待普通池，不新建root/结果窗，
也不等待被取消任务或慢socket退出。calling control连接持有自己的有界协议能力；
目标 statement 的取消、实际计算cut、closing与generation收尾仍按原协议owner推进。

新增用例把 root/business/Local window 都填满，再关闭ordinary admission；
未授权KILL不修改target，授权KILL仍返回OK并登记目标取消，不增加root/business/window。
SET不进入controlroute。最后target owner退出释放原位置。
Query Application lib518 PASS；Frontend lib1435 PASS。日志：
`logs/mem-1-m07/p07-control-route-query-final-20261007.log`、
`logs/mem-1-m07/p07-control-route-fe-20261007.log`。新fixture首次错用
CancellationView::is_cancelled，改用原reason API；产品未改取消语义。

该用例不替代C7/P09真实MySQL socket、独立control连接满载、closing pool与
下一generation探针；完整production purpose/window、Local aliases、P08旧路径退出、
P00b/P09/P10仍OPEN。local only，未push/PR。
