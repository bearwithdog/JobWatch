#__author: liutengyu
#__Date:2018/6/25
import re
path_rwfile='C:/Users/Administrator/Desktop/01上线工作目录lty/'
#a系统名 b表名 c主键 line[3]算法
def write(query,switcher,writer):
    for line in query:
        print(len(query))
        line=line.replace('\n','')
        line=line.split('\t')
        print(line)
        a=line[0]
        ah=a[:1]+"H"+a[1:]
        b=line[1]
        c=line[2]
        sh_py=ah+'_'+b+'0200.py'
        sh_py=sh_py.lower()
        print(a,b,c)
        if line[3] in switcher:
            PK=switcher[line[3]]
            for i in range(2):
                PK=PK.replace('$a$',a)
                PK=PK.replace('$ah$',ah)
                PK=PK.replace('$b$',b)
                PK=PK.replace('$c$',c)
                PK=PK.replace('$sh_py$',sh_py)
            ####################################
                lis_c=c.split(',')
                Ac_Bc=[]
                for i_c,j in zip(lis_c,range(len(lis_c))):
                    lis_c[j]='A.'+i_c
                    lis_b='B.'+i_c
                    Ac_Bc.append('A.'+i_c+'='+lis_b)
                Ac=','.join(lis_c)
                Ac_Bc=','.join(Ac_Bc)
                PK=PK.replace('$A.c$',Ac)
                PK=PK.replace('$A.c=B.c$',Ac_Bc)
            ######################################
                print(PK)
                writer.write(PK+'\n')
                if line[3]+'_H' in  switcher:
                    PK=switcher[line[3]+'_H']
                else:
                    break



def PCheckSA():#主键检测
    switch_P_SA={
    'I':"UNION ALL SELECT '$a$_$b$','主键重复',TAB.CNT FROM (SELECT $c$,COUNT(*) AS CNT FROM SDATA.$a$_$b$ WHERE DT=\'$bizDate$\' GROUP BY $c$ HAVING CNT>1) TAB",
    'F3':"UNION ALL SELECT '$a$_$b$',\'主键重复\',TAB.CNT FROM (SELECT $c$,COUNT(*) AS CNT FROM SDATA.$a$_$b$ WHERE DT=\'$bizDate$\' GROUP BY $c$ HAVING CNT>1) TAB",
    'F5':"UNION ALL SELECT '$a$_$b$',\'主键重复\',TAB.CNT FROM (SELECT $c$,COUNT(*) AS CNT FROM SDATA.$a$_$b$ WHERE DT=\'$bizDate$\' GROUP BY $c$ HAVING CNT>1) TAB"
    }
    checkcode_sdatapk2 = open(path_rwfile+'config.conf', "a", encoding="utf-8")
    checkcode_sdatapk2.write('PrimaryCheckSA.conf:\n')
    write(query,switch_P_SA,checkcode_sdatapk2)
    checkcode_sdatapk2.close()

    
def PCheckSH():
    switch_P_SH={
    'I':"UNION ALL SELECT '$ah$_$b$' ,'主键重复',TAB.CNT FROM (SELECT COUNT(*) AS CNT FROM SHDATA.$ah$_$b$  GROUP BY $c$ HAVING CNT>1) TAB",
    'F3':"UNION ALL SELECT '$ah$_$b$' ,'主键重复',TAB.CNT FROM (SELECT COUNT(*) AS CNT FROM SHDATA.$ah$_$b$  GROUP BY $c$ HAVING CNT>1) TAB",
    'F5':"UNION ALL SELECT '$ah$_$b$' ,'主键重复',TAB.CNT FROM (SELECT COUNT(*) AS CNT FROM SHDATA.$ah$_$b$  GROUP BY $c$ HAVING CNT>1) TAB",
    'F3_H':"UNION ALL SELECT '$ah$_$b$_H' ,'主键重复',TAB.CNT FROM (SELECT COUNT(*) AS CNT FROM SHDATA.$ah$_$b$_H GROUP BY $c$,start_dt HAVING CNT>1) TAB",
    'F5_H':"UNION ALL SELECT '$ah$_$b$_H' ,'主键重复',TAB.CNT FROM (SELECT COUNT(*) AS CNT FROM SHDATA.$ah$_$b$_H GROUP BY $c$,start_dt HAVING CNT>1) TAB"
    }
    checkcode_sdatapk2 = open(path_rwfile+'config.conf', "a", encoding="utf-8")
    checkcode_sdatapk2.write('PrimaryCheckSH.conf:\n')
    write(query,switch_P_SH,checkcode_sdatapk2)
    checkcode_sdatapk2.close()

def CheckSASH():
    switch_SASH={
    'I':"UNION ALL SELECT sa.name,\'数据sa与sh比对\',1 FROM (SELECT count(*) num,'$b$' name  FROM SDATA.$a$_$b$  ) AS sa JOIN (SELECT count(*) num,'$b$' name  FROM SHDATA.$ah$_$b$ ) AS sh ON sa.name=sh.name WHERE sa.num <> sh.num",
    'F3':"UNION ALL SELECT sa.name,\'数据sa与sh比对\',1 FROM (SELECT count(*) num,'$b$' name  FROM SDATA.$a$_$b$  where dt =\'$bizDate$\') AS sa JOIN(SELECT count(*) num,'$b$' name  FROM SHDATA.$ah$_$b$ ) AS sh ON sa.name=sh.name JOIN (SELECT count(*) num,'$b$' name  FROM SHDATA.$ah$_$b$_H where end_dt=\'3000-12-31\' and start_dt=\'$bizDate10$\' ) AS sh_h ON sh.name = sh_h.name WHERE sa.num <> sh.num or   sh.num <> sh_h.num OR   sa.num <> sh_h.num",
    'F5':"UNION ALL SELECT sa.name,\'数据sa与sh比对\',1 FROM (SELECT count(*) num,'$b$' name  FROM SDATA.$a$_$b$  where dt =\'$bizDate$\') AS sa JOIN(SELECT count(*) num,'$b$'  name  FROM SHDATA.$ah$_$b$ ) AS sh ON sa.name=sh.name JOIN (SELECT count(*) num,'$b$'  name  FROM SHDATA.$ah$_$b$_H where end_dt=\'3000-12-31\' ) AS sh_h ON sh.name = sh_h.name WHERE sa.num <> sh.num or   sh.num <> sh_h.num OR   sa.num <> sh_h.num"
    }
    checkcode_sdatapk2 = open(path_rwfile+'config.conf', "a", encoding="utf-8")
    checkcode_sdatapk2.write('sash.conf:\n')
    write(query,switch_SASH,checkcode_sdatapk2)
    checkcode_sdatapk2.close()

def CheckCross():
    switch_Cross={
    'F3':"UNION ALL SELECT '$ah$_$b$_H',\'交叉链\',TAB.CNT FROM (SELECT $A.c$,COUNT(*) AS CNT FROM shdata.$ah$_$b$_H A LEFT JOIN SHDATA.$ah$_$b$_H B ON $A.c=B.c$ AND A.START_DT = B.END_DT WHERE B.END_DT IS NULL GROUP BY $A.c$ HAVING COUNT(*)>1) TAB",
    'F5':"UNION ALL SELECT '$ah$_$b$_H',\'交叉链\',TAB.CNT FROM (SELECT $A.c$,COUNT(*) AS CNT FROM shdata.$ah$_$b$_H A LEFT JOIN SHDATA.$ah$_$b$_H B ON $A.c=B.c$ AND A.START_DT = B.END_DT WHERE B.END_DT IS NULL GROUP BY $A.c$ HAVING COUNT(*)>1) TAB"
    }
    checkcode_sdatapk2 = open(path_rwfile+'config.conf', "a", encoding="utf-8")
    checkcode_sdatapk2.write('CrossChainSH.conf:\n')
    write(query,switch_Cross,checkcode_sdatapk2)
    checkcode_sdatapk2.close()

def CheckEff():
    switch_Eff={
    'F3':"UNION ALL SELECT  T.NAME,\'有效数据检测\',T.NUM FROM (SELECT \'$ah$_$b$_H\' AS NAME,COUNT(*) AS NUM FROM SHDATA.$ah$_$b$_H WHERE  START_DT>END_DT) T WHERE T.NUM!=0",
    'F5':"UNION ALL SELECT  T.NAME,\'有效数据检测\',T.NUM FROM (SELECT \'$ah$_$b$_H\' AS NAME,COUNT(*) AS NUM FROM SHDATA.$ah$_$b$_H WHERE  START_DT>END_DT) T WHERE T.NUM!=0"
    }
    checkcode_sdatapk2 = open(path_rwfile+'config.conf', "a", encoding="utf-8")
    checkcode_sdatapk2.write('ShEffective.conf:\n')
    write(query,switch_Eff,checkcode_sdatapk2)
    checkcode_sdatapk2.close()
def CheckEdepen():
    switch_dep={
    'I' :"INSERT INTO etl.ETL_JOB_DEPENDENCE_CONTROL VALUES ('SHDATA.$ah$_$b$','SDATA.$a$_$b$','SHDATA','$a$','$ah$_$b$','SHDATA.$ah$_$b$','$sh_py$','','','')",
    'F3':"INSERT INTO etl.ETL_JOB_DEPENDENCE_CONTROL VALUES ('SHDATA.$ah$_$b$','SDATA.$a$_$b$','SHDATA','$a$','$ah$_$b$','SHDATA.$ah$_$b$','$sh_py$','','','')",
    'F5':"INSERT INTO etl.ETL_JOB_DEPENDENCE_CONTROL VALUES ('SHDATA.$ah$_$b$','SDATA.$a$_$b$','SHDATA','$a$','$ah$_$b$','SHDATA.$ah$_$b$','$sh_py$','','','')"
    }
    #INSERT INTO etl.ETL_JOB_DEPENDENCE_CONTROL VALUES ('SHDATA.SH001_OUT_DATA_WC_GJJ','SDATA.S001_OUT_DATA_WC_GJJ','SHDATA','S001','SH001_OUT_DATA_WC_GJJ','SHDATA.SH001_OUT_DATA_WC_GJJ','sh001_out_data_wc_gjj0200.py','','','')
    checkcode_sdatapk2 = open(path_rwfile+'config.conf', "a", encoding="utf-8")
    checkcode_sdatapk2.write('dependence.conf:\n')
    write(query,switch_dep,checkcode_sdatapk2)
    checkcode_sdatapk2.close()

def CheckStrategy():
    checkcode_sdatapk2 = open(path_rwfile+'config.conf', "a", encoding="utf-8")
    checkcode_sdatapk2.write('TableStrategy:\n')
    for line in query:
        line=line.split('\t')
        checkcode_sdatapk2.write(line[1]+':'+line[3]+'\n')
        checkcode_sdatapk2.write(line[0]+'_'+line[1]+r"_FIELDS_TERMINATE='\001'")
    checkcode_sdatapk2.write('\n\n')
    checkcode_sdatapk2.close()

def TaskctlSet():
    checkcode_sdatapk2 = open(path_rwfile+'config.conf', "a", encoding="utf-8")
    checkcode_sdatapk2.write('\n\n\nTaskctlSet:\n')
    JobMOdSH='''
        <python>
            <name>SH$sys$_$tablenameE$$tablenameC$</name>
            <progname>$(sh$sys$Path)sh$sys$_$tablenameS$0200.py</progname>
            <para>$(bizDate)</para>
        </python>
        '''
    JobMOdSHH='''
        <python>
            <name>SH$sys$_$tablenameE$_H$tablenameC$</name>
            <progname>$(sh$sys$Path)sh$sys$_$tablenameS$_h0200.py</progname>
            <para>$(bizDate)</para>
        </python>
        '''
    JobMOdSA='''
        <python>
            <name>S$sys$_$tablenameE$$tablenameC$</name>
            <progname>$(filePath)</progname>
            <para>S$sys$ $tablenameE$ $(bizDate) $para$</para>
        </python>
        '''
    def repl(ss):
        ss=ss.replace('$sys$',sys)
        ss=ss.replace('$tablenameE$',tablenameE)
        ss=ss.replace('$tablenameC$',tablenameC)
        ss=ss.replace('$tablenameS$',tablenameS)
        ss=ss.replace('$para$',para)
        return ss
    
    def SetSA():
        nonlocal para
        #global para
        ss=JobMOdSA
        if line[3]=='I' or line[3]=='F3':
            para='1 "'+line[5]+'" null $(bizDate)'
            checkcode_sdatapk2.write(repl(ss))
        else:
            para='0 null null null'
            checkcode_sdatapk2.write(repl(ss))
    
    def SetSH():
        ss=JobMOdSH
        seriel_name="\n        <name>"+line[0]+"_"+line[1]+"</name>"
        if line[3]=='I':
            tmp='<serial>'+seriel_name+repl(ss)+'</serial>\n'
            checkcode_sdatapk2.write(tmp)
        else:
            tmp=repl(ss)+repl(JobMOdSHH)
            tmp='<serial>'+seriel_name+tmp+'</serial>\n'
            checkcode_sdatapk2.write(tmp)
    
    for i in range(2):
        if i==0:
            for line in query:
                print (line)
                line=line.replace('\n','')
                line=line.split('\t')
                sys=re.search('[0-9]{3}',line[0], flags=0).group()
                tablenameE=line[1].upper()
                tablenameC=line[4]
                tablenameS=line[1].lower()
                para=''
                SetSA()
        else:
            for line in query:
                line=line.replace('\n','')
                line=line.split('\t')
                sys=re.search('[0-9]{3}',line[0], flags=0).group()
                tablenameE=line[1].upper()
                tablenameC=line[4]
                tablenameS=line[1].lower()
                para=''
                SetSH()   
    checkcode_sdatapk2.close()
checkcode_sdatapk2 = open(path_rwfile+'onlinetable.txt', "r",encoding='utf-8')
query=checkcode_sdatapk2.readlines()
print(query)
function=[CheckStrategy,PCheckSA,PCheckSH,CheckSASH,CheckCross,CheckEff,CheckEdepen,TaskctlSet]
for fun in function[:]:
    fun()
