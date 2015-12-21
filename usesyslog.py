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
        return 
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
    elif s.find(u"材料")!=-1:
        return s[:s.index(u"材料")+2]
    elif s.find(u"东中院")!=-1:
        return s[:s.index(u"东中院")+3]
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
    return (t[-1],time.strftime("%Y-%m-%d %H:%M:%S",c)+"|"+str(float(t[2])-float(t[1]))+"|"+t[4])
if __name__ == "__main__":
    holiday=['2014-10-01', '2014-10-02', '2014-10-03', '2015-01-01', '2015-01-19', '2015-01-20', '2015-01-21', '2015-01-22', '2015-01-23', '2015-01-24', '2015-01-25', '2015-01-26', '2015-01-27', '2015-01-28', '2015-01-29', '2015-01-30', '2015-01-31']
    sc = SparkContext(appName="PythonWordCount")
    wifiacc = sc.textFile("/user/omnilab/warehouse/WifiUsers", 1).map(lambda x:(x.split("|")[0],x.split("|")[1]))
    wifiinfo = sc.textFile("/user/omnilab/warehouse/WifiSyslogSession/wifilog2014-*",1)\
            .filter(lambda x:float(x.split(",")[2])-float(x.split(",")[1])>60000)\
            .map(lambda x:timeconverter(x))
    wifiinfo2 = sc.textFile("/user/omnilab/warehouse/WifiSyslogSession/wifilog2015-01-*",1)\
            .filter(lambda x:float(x.split(",")[2])-float(x.split(",")[1])>60000)\
            .map(lambda x:timeconverter(x))
    wifiinfo=wifiinfo.union(wifiinfo2)
    # now the format is (student number|format time|weekday number)
    wifiinfo=wifiinfo.filter(lambda x:x[1].split("|")[0]>"2014-09-13" and x[1].split("|")[0] not in holiday and u"徐汇" not in x[1].split("|")[2] and u"法华" not in x[1].split("|")[2])\
            .join(wifiacc)\
            .map(lambda x: x[1][1]+"|"+x[1][0])\
            .distinct()\
            .sortBy(lambda x: x)
    wifiinfo.saveAsTextFile(sys.argv[1])
    print("******************OK***********************")
    sc.stop()