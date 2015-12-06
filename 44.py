# -*- coding:utf-8 -*- 
import sys
import time
from operator import add
from pyspark import SparkContext

def func(s):
    if s.find(u"食堂")!=-1:
        s=s.replace(u"食堂2",u"食堂")
        s=s.replace(u"新","")
        s=s.replace(u"闵行","")
        s=s.replace(u"五餐",u"第五")
        return s
    elif s.find(u"南区体育馆")!=-1:
        return s.replace(u"南区体育馆",u"西南体育馆-南体")
    elif s.find(u"综合体育馆")!=-1:
        return s.replace(u"综合体育馆",u"新体育馆-近沧源路")
    elif s.find(u"致远游泳健身馆")!=-1:
        return s[:s.index(u"健身馆")+3]
    elif s.find(u"网球场"):
        return s.replace(u"网球场",u"学生服务中心")
    else: 
        return s
def func2(x):
    s=x.split("|")
    return (s[0],s[3]+","+s[1])
def func3(x):
    s=x.split("\t")
    return (s[1],s[1]+","+s[2]+","+s[3]+","+s[4]+","+s[5])

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8') 
    sc = SparkContext(appName="PythonWordCount")
    al = sc.textFile("./NET/wifi_user", 1)
    al=al.filter(lambda x: float(x.split("|")[2])>60000)\
            .map(func2)
    info=sc.textFile("./EMC/account.txt", 1).map(func3)
    al=al.join(info).map(lambda x: x[1][1]+","+x[1][0])
    card=sc.textFile("./NET/trade",1)
    card=card.map(func).filter(lambda x:x.find(u"水")==-1).map(lambda x:(x.split(",")[0],x.split(",")[1]))
    info=info.join(card).map(lambda x: x[1][0]+","+x[1][1].split("\t")[1]+","+x[1][1].split("\t")[0]).distinct()
    info=info.union(al).sortBy(lambda x: x.split(",")[0]+x.split(",")[-1])
    info.saveAsTextFile("./NET/tradefilt")
    print("******************OK***********************")
    sc.stop()
