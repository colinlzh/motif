# -*- coding:utf-8 -*- ＃
import sys
import time
from operator import add
from pyspark import SparkContext

def func(s):
    if s.find(u"陈瑞球")!=-1:
        return s[:s.index(u"陈瑞球")+3]
    elif s.find(u"图书馆四区")!=-1:
        s=s.replace(u"图书馆四区",u"图书馆")
        return s
    elif s.find(u"媒体与设计实验室")!=-1:
        return s[:s.index(u"媒体与设计实验室")+8]
    elif s.find(u"机械与动力工程学院")!=-1:
        return s[:s.index(u"机械与动力工程学院")+9]
    elif s.find(u"东中院")!=-1:
        return s[:s.index(u"东中院")+3]
    elif s.find(u"校医院")!=-1:
        return s[:s.index(u"校医院")+3]
    elif s.find(u"药学楼")!=-1:
        return s[:s.index(u"药学楼")+3]
    elif s.find(u"电信群楼")!=-1:
        return s[:s.index(u"电信群楼")+4]
    elif s.find(u"木兰学院")!=-1:
        return s[:s.index(u"木兰学院")+4]
    elif s.find(u"分析测试中心")!=-1:
        return s[:s.index(u"分析测试中心")+6]
    elif s.find(u"农学生物学院")!=-1:
        return s[:s.index(u"农学生物学院")+6]
    else: 
        return s
def timeconverter(s):
    t=s.split(",")
    c=time.localtime(float(t[1])/1000)
    return time.strftime("%Y-%m-%d %H:%M:%S",c)+"|"+str(float(t[2])-float(t[1]))+"|"+t[3]
if __name__ == "__main__":
    holiday=['2014-10-01', '2014-10-02', '2014-10-03', '2015-01-01', '2015-04-05', '2015-05-01', '2015-01-19', '2015-01-20', '2015-01-21', '2015-01-22', '2015-01-23', '2015-01-24', '2015-01-25', '2015-01-26', '2015-01-27', '2015-01-28', '2015-01-29', '2015-01-30', '2015-01-31', '2015-02-01', '2015-02-02', '2015-02-03', '2015-02-04', '2015-02-05', '2015-02-06', '2015-02-07', '2015-02-08', '2015-02-09', '2015-02-10', '2015-02-11', '2015-02-12', '2015-02-13', '2015-02-14', '2015-02-15', '2015-02-16', '2015-02-17', '2015-02-18', '2015-02-19', '2015-02-20', '2015-02-21', '2015-02-22', '2015-02-23', '2015-02-24', '2015-02-25', '2015-02-26', '2015-02-27', '2015-02-28']
    sc = SparkContext(appName="PythonWordCount")
    wifiacc = sc.textFile("/user/omnilab/warehouse/sjtu_wifi_users", 1).map(lambda x:(x.split("|")[0],x.split("|")[1]))
    wifiinfo= sc.textFile("/user/omnilab/warehouse/sjtu_wifi_syslog_clean/wifilog2014-09.clean",1)\
            .map(lambda x:(x.split(",")[5],timeconverter(x)))\
            .join(wifiacc)\
            .map(lambda x: x[1][1]+"|"+x[1][0])
            # .map(lambda x: x[1][1].split("|")[1]+"|"+x[1][0])
    for i in range(10,14):
        if i>12:
            wifiinfo2=sc.textFile("/user/omnilab/warehouse/sjtu_wifi_syslog_clean/wifilog2015-0"+str(i-12)+".clean",1)
        else:
            wifiinfo2=sc.textFile("/user/omnilab/warehouse/sjtu_wifi_syslog_clean/wifilog2014-"+str(i)+".clean",1)
        wifiinfo2=wifiinfo2.map(lambda x:(x.split(",")[5],timeconverter(x)))\
                    .join(wifiacc)\
                    .map(lambda x: x[1][1]+"|"+x[1][0])
        wifiinfo=wifiinfo.union(wifiinfo2)
    # now the format is (student number|format time|weekday number)
    wifiinfo=wifiinfo.distinct()\
                .filter(lambda x:x.find(u"徐汇")==-1 and x.find(u"法华")==-1 and x.find(u"七宝")==-1 and x.find("null")==-1)\
                .map(func)\
                .filter(lambda x: x.split("|")[1].split(" ")[0]<"2015-01-20")\
                .sortBy(lambda x:x.split("|")[0]+x.split("|")[1])
    wifiinfo.saveAsTextFile(sys.argv[1])
    print("******************OK***********************")
    sc.stop()