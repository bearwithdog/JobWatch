# JobWatch
作业实时监控，中间坑的一些总结。  
## 作业实时监控实现思想  
解析后台ETL调度工具产生的日志，解析日志获得最新的作业运行状态，将  
最新的作业运行状态插入到跑批监控表中。  
## 程序执行过程  
### 1、程序启动  
查询监控表中是否存在今日跑批的数据，如果不存在就初始化监控表，抓取所有待执行的作业名称，  
并将其运行状态置为0，表明作业未开始。之后程序进入抓取运行作业状态同时维护该数据表的流程中。
如果存在今日跑批的数据，证明今天已经初始化过作业，不再进行初始化。直接进入维护该数据表的流程中。  
### 2、程序扫描作业
使用linux命令行获取最近被修改过的文件，即抓取最近的被调度工具修改过的日志文件，打开该文件提取其关键  
数据，包含不限于作业开始时间，作业结束时间，作业运行日志的最后一行，使用update语句更新表。
### 3、程序退出
ETL调度工具按照流程划分作业集，每一个流程完毕，都会在该流程下的文件夹中ctlcore.log文件中打印一条流程归档  
成功。程序退出的检测思路为，每次监控程序跟随跑批启动，记录下该程序的启动时间，在每个扫描间隔中不断查找各个流程的  
归档成功时间，当所有流程归档成功且归档时间大于流程启动时间，证明所有流程运行完毕。监控程序退出。同时将强制通过的作业  
最终状态置为14代表警告通过。


状态码：
0:作业未开始 1:作业运行中 10:作业运行错误 11:作业正常完成 14:作业警告通过
