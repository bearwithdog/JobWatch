#!/usr/local/python/bin/python
# coding=utf-8
#find . -name '*.phtml' -type f -mmin -30
import sys
import os
import time
import commands 
import io
import time
import os.path
import MySQLdb
import datetime
import re
##############全局变量设置##################
Jobinfo=[] #初始化作业列表
JobDic={}  #作业字典表
Flow={} #统计一共多少个不同的flow，用于抓取scanEXIT
JobInterval=1#扫描N分钟之前的作业
ScanInterval=10#扫描时间间隔单位秒
ProgInterval=0#单位分钟
check_time=datetime.datetime.now()+datetime.timedelta(minutes=ProgInterval) #初始化check_time,对check_time直接加两个小时
StartTime=datetime.datetime.now().strftime("%m%d%H%M%S")
EDW_HOME=os.environ['EDW_HOME'] #获取系统
CHECKDATA_DIR=EDW_HOME+'/CHECKDATA/' #程序所在路径
Tmp_Dir=EDW_HOME+'/tmp/' #临时文件存在路径
mysqldb='50.16.0.102' #生产
#mysqldb='192.168.3.41'  #测试
###########################################
if len(sys.argv)<2:
    print("不给跑批日期参数跑个屁啊！")
    sys.exit(1)
BizData=sys.argv[1]
###########################################
########作业信息初始化流程、模块、作业#######
def InitJob():
    (stats,output)= commands.getstatusoutput("sh "+CHECKDATA_DIR+"etl_log.sh") 
    global Jobinfo
    global JobDic
    FlowNameCN=''
    ModuleCN=''
    FlowNameID=''
    fil=open(Tmp_Dir+'TaskctlJobDic.txt','w')
    output=output.decode('gbk').encode('utf-8')
    fil.write(output)
    fil.close()
    fil=open(Tmp_Dir+'TaskctlJobDic.txt','r')
    output=fil.readlines()
    fil.close()
    #####解析etl_log.sh脚本抓取到的作业####
    for tmp in output:
        if '#begin' in tmp:
            FlowNameCN=tmp.split(' ')[1]
            FlowNameID=tmp.split(' ')[2]
            ModuleCN=tmp.split(' ')[3]
            if FlowNameID not in Flow:
                Flow[FlowNameID]='1' #初始化Flow信息，在ScanExit中检测如果全部为0，则程序退出
            continue
        if '#end' not in tmp and tmp!='\n' and 'flowinfo.fxm:' not in tmp:
            JobName=tmp
            ##############一定要注意去掉空白符，显示不出来因为键是有空白符的，所以会一直找不到字典中的该key#############
            JobName=''.join(JobName.split())#去掉空白符
            JobName=JobName.strip()
            ####################################################################################################
            stats=-1
            Jobinfo.append([FlowNameID,FlowNameCN,ModuleCN,JobName,stats])
            JobDic[JobName]=FlowNameID+','+FlowNameCN+','+ModuleCN
       
########扫描动作############
def Scan():
    global JobDic
    time.sleep(ScanInterval) #睡眠一分钟
    (stats,output)= commands.getstatusoutput("find $TASKCTLDIR/work/log/  -name '*.log' -type f -mmin -%s" % JobInterval) #找出两分钟内有更新的日志
    fil=open(Tmp_Dir+'Taskctltemp.txt','w')
    output=output.decode('gbk').encode('utf-8')
    fil.write(output)
    fil.close()
    fil=open(Tmp_Dir+'Taskctltemp.txt','r')
    output=fil.readlines()
    fil.close()
    for FileName in output:
        BeginTime=[]
        RuningRes=[]
        EndStats=[]
        EndTime=[]
        JobName=''
        ParaName=''
        if 'ctlcore.log' in FileName:
            continue
        JobName=FileName.split('/')
        JobName=JobName[len(JobName)-1].split('.')[0]
        FileName=FileName.replace('\n','')
        #####TASKCTL日志是GBK编码读进来后进行GBK解码再编码为UTF-8使用方便#####
        File=open(FileName.decode('utf-8').encode('gbk'),'r')
        contents=File.readlines()
        for cont in contents:
            try:
                cont=cont.decode('gbk').encode('utf-8')
            except:
                cont=cont
            cont=cont.replace('\n','')
            if '准备时间:' in cont :
                BeginTime.append(cont)
            #开始时间不存在时，获取准备时间作为开始时间
            # elif '准备时间' in cont:
            #     BeginTime.append(cont)
            elif '执行结果' in cont:
                RuningRes.append(cont)
            elif '结束状态' in cont:
                EndStats.append(cont)
            elif '结束时间:' in cont:
                EndTime.append(cont)
            elif '程序名称' in cont:
                ProgName=cont
            elif '程序参数' in cont:
                Para=cont
            elif '调度批次' in cont:
                Batch=cont
            elif '作业类型' in cont:
                JobType=cont
        try:
            ###尝试查找初始化字典表中的作业信息，若找不到抛出异常####
            TempJobstats=JobDic[JobName].split(',')
            New_Jobinfo=[]
            #TempJobstats[0]流程ID，TempJobstats[1]流程中文名，TempJobstats[2]模块名称
            JOB_FLOW_ID=TempJobstats[0]
            JOB_FLOW_NAME=TempJobstats[1]
            MOUDLE_NAME=TempJobstats[2]
            JOB_NAME=JobName
            BATCH_PRD=Batch.split(':')[1][:8]
            JOB_TYPE=JobType.split(':')[1]
            PROG_NAME=ProgName.split(':')[1]
            PROG_PARA=Para.split(':')[1]
            JOB_START_TIME=BeginTime[len(BeginTime)-1]
            JOB_END_TIME='3000-12-31'
            RUN_STS='1'
            ERR_LOG='正在执行无法获取日志'
            #开始时间的个数大于结束时间证明该脚本还在运行
            if len(EndTime)<len(BeginTime):
                RUN_STS='1'
            elif len(BeginTime)==len(EndTime):
                #程序已经结束更新运行状态，更新结束时间,以及日志最后一行有效数据提取#
                RUN_STS=EndStats[len(EndStats)-1].split(':')[1].split('-')[0]
                JOB_END_TIME=EndTime[len(EndTime)-1]
                for i in range(len(contents))[::-1]:#取出最后一行不为空的值
                    if contents[i].replace('\n','')!='':
                        ERR_LOG=contents[i].replace('\n','').decode('gbk').encode('utf-8')
                        break
            New_Jobinfo=[JOB_FLOW_ID,JOB_FLOW_NAME,MOUDLE_NAME,JOB_NAME,BATCH_PRD,JOB_TYPE,PROG_NAME,PROG_PARA,JOB_START_TIME,JOB_END_TIME,RUN_STS,ERR_LOG]
            UpdateMysql(New_Jobinfo)
            #######该出可以替换为插入数据的SQL######
            # s=''
            # for i in New_Jobinfo:
            #     s=s+i+','
            # print(s)
        except:
            ######异常处理，由于扫描出的作业不是要监控的作业而是Taskctl内置作业故不做操作直接跳过#####
            #print("程序111行抛出异常："+JobName)
            pass

def LinkMysql():
# 打开数据库连接,一定要指定Mysql的链接方式是不是utf8的，否则提交的中文会变成乱码，编码方式一定要与环境统一
    conn= MySQLdb.connect(
        host=mysqldb,
        port = 3306,
        user='etl',
        passwd='etl',
        db ='etl',
        charset='utf8'
        )
    # 使用cursor()方法获取操作游标 
    cur=conn.cursor()
    return (cur,conn) 

def IinitMysql(cur,conn):
    global JobDic
    sql_modl='''insert into etl.ETL_RUN_BCH_JOB_LOG (
    JOB_FLOW_ID
    ,JOB_FLOW_NAME
    ,MOUDLE_NAME
    ,JOB_NAME
    ,BATCH_PRD
    ,JOB_TYPE
    ,PROG_NAME
    ,PROG_PARA
    ,JOB_START_TIME
    ,JOB_END_TIME
    ,RUN_STS
    ,ERR_LOG
    )
    VALUES
    $Value$;'''
    #TempJobstats[0]流程ID，TempJobstats[1]流程中文名，TempJobstats[2]模块名称
    Value=''
    i=0
    for key in JobDic:
        i=i+1
        if i%100==0:
            sql=sql_modl.replace('$Value$',Value)
            sql=sql.decode('utf-8').encode('utf-8')
            cur.execute(sql)
            conn.commit()
            print(sql_modl.replace('$Value$',Value))
            Value=''
        TempJobstats=JobDic[key].split(',')
        if Value!='':
            Value=Value+','
        Value=Value+"('"+TempJobstats[0]+"',"+"'"+TempJobstats[1]+"',"+"'"+TempJobstats[2]+"',"+"'"+key+"',"+"'"+BizData+"',"+"'','','','','','0','')"
    sql=sql_modl.replace('$Value$',Value)
    sql=sql.decode('utf-8').encode('utf-8')
    print(sql_modl.replace('$Value$',Value))
    cur.execute(sql)
    conn.commit()
    
    
    
def UpdateMysql(New_Jobinfo):
    global cur
    global conn
    #sql="update etl.ETL_RUN_BCH_JOB_LOG set RUN_STS='"+New_Jobinfo[10]+"' where JOB_NAME='"+New_Jobinfo[3]+"' and MOUDLE_NAME='"+New_Jobinfo[2]+"'"
    sql='''update etl.ETL_RUN_BCH_JOB_LOG set 
    JOB_TYPE='$JOB_TYPE$',
    PROG_NAME='$PROG_NAME$',
    PROG_PARA='$PROG_PARA$',
    JOB_START_TIME='$JOB_START_TIME$',
    JOB_END_TIME='$JOB_END_TIME$',
    RUN_STS='$RUN_STS$',
    ERR_LOG='$ERR_LOG$' 
    where JOB_NAME='$JOB_NAME$' and JOB_FLOW_ID='$JOB_FLOW_ID$' and BATCH_PRD='$BATCH_PRD$';
    '''       
    ###对单引号添加转义\  
    New_Jobinfo[7]=New_Jobinfo[7].replace("'","\\'")
    New_Jobinfo[11]=New_Jobinfo[11].replace("'","\\'")
    UpdateSql=sql
    UpdateSql=UpdateSql.replace('$JOB_TYPE$',New_Jobinfo[5])
    UpdateSql=UpdateSql.replace('$PROG_NAME$',New_Jobinfo[6])
    UpdateSql=UpdateSql.replace('$PROG_PARA$',New_Jobinfo[7])
    UpdateSql=UpdateSql.replace('$JOB_START_TIME$',New_Jobinfo[8].split(':',1)[1])
    ###END_TIME_TMP使用冒号分隔一次，如果没有冒号则不分隔
    END_TIME_TMP=New_Jobinfo[9].split(':',1)
    ###取分隔后的后面一个元素作为结束时间
    UpdateSql=UpdateSql.replace('$JOB_END_TIME$',END_TIME_TMP[len(END_TIME_TMP)-1])
    UpdateSql=UpdateSql.replace('$RUN_STS$',New_Jobinfo[10])  
    UpdateSql=UpdateSql.replace('$ERR_LOG$',New_Jobinfo[11])
    UpdateSql=UpdateSql.replace('$JOB_NAME$',New_Jobinfo[3])
    UpdateSql=UpdateSql.replace('$JOB_FLOW_ID$',New_Jobinfo[0])
    UpdateSql=UpdateSql.replace('$BATCH_PRD$',BizData)
    print(UpdateSql)
    cur.execute(UpdateSql)
    conn.commit()

def ScanExit():
    global check_time
    global Flow
    global StartTime
    global conn
    #由于要执行的命令是带中文的所以这里一定要把系统编码设置为GBK
    #nowTime=datetime.datetime.now().strftime("%m%d%H%M%S")
    if datetime.datetime.now()>=check_time:  
        for flow in Flow:
            command="grep 'fdc_cycle' $TASKCTLDIR/work/log/%s/ctlcore.log" % flow
            (stats,output)= commands.getstatusoutput(command)
            pattern = re.compile(r'\d+')
            res = re.findall(pattern, output) #res归档时间
            if res :
                #归档时间大于监控开始时间该流程结束
                #2018年9月7日发生数组访问越界，此处加上异常处理，再次运行查找错误原因
                try:
                    guidangTIME=res[0]+res[1]
                    #print(guidangTIME)
                    if int(guidangTIME)>int(StartTime):
                        Flow[flow]='0'
                except Exception as e:
                    print(str(e),'res:',str(output))
        if '1' not in Flow.values():
            print("所有流程归档成功,监控程序正在刷新数据，请稍后。。。")
            #退出之前将所有强制通过任务的结束状态置为14警告通过
            sql="update etl.ETL_RUN_BCH_JOB_LOG set RUN_STS='14' where RUN_STS='10'"
            cur.execute(sql)
            conn.commit()
            print("刷新数据完毕正在退出")
            print("正在关闭数据库连接")
            try:
                conn.close()
            except Exception as e:
                print(e)
            time.sleep(2)
            sys.exit(0)
        else :
            print("检测到有流程正在运行，程序不退出")
        #对时间戳加ProgInterval分钟，得到下一次进行程序退出检测的时间
        check_time=datetime.datetime.now()+datetime.timedelta(minutes=ProgInterval)





InitJob()
tmp=LinkMysql()
cur=tmp[0]#获取游标
conn=tmp[1]#获取链接

cur.execute("SELECT COUNT(1) from etl.ETL_RUN_BCH_JOB_LOG WHERE BATCH_PRD=%s" % BizData)
result=cur.fetchall() 
if result[0][0]==0:#检测是否需要初始化
    IinitMysql(cur,conn)
    print("##################%s作业初始化状态已经完成######################" % BizData)


print("##################监控启动##########################")
while 1:
    Scan()
    ScanExit()

