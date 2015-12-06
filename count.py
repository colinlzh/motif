# -*- coding:utf-8 -*- ＃
import sys
import time
from operator import add
from pyspark import SparkContext

def timeconverter(s):
    t=s.split(",")
    c=time.localtime(float(t[1])/1000)
    return time.strftime("%Y-%m-%d %H:%M:%S",c)+"|"+str(c.tm_wday+1)
if __name__ == "__main__":
    holiday=['2014-10-01', '2014-10-02', '2014-10-03', '2015-01-01', '2015-04-05', '2015-05-01', '2015-01-19', '2015-01-20', '2015-01-21', '2015-01-22', '2015-01-23', '2015-01-24', '2015-01-25', '2015-01-26', '2015-01-27', '2015-01-28', '2015-01-29', '2015-01-30', '2015-01-31', '2015-02-01', '2015-02-02', '2015-02-03', '2015-02-04', '2015-02-05', '2015-02-06', '2015-02-07', '2015-02-08', '2015-02-09', '2015-02-10', '2015-02-11', '2015-02-12', '2015-02-13', '2015-02-14', '2015-02-15', '2015-02-16', '2015-02-17', '2015-02-18', '2015-02-19', '2015-02-20', '2015-02-21', '2015-02-22', '2015-02-23', '2015-02-24', '2015-02-25', '2015-02-26', '2015-02-27', '2015-02-28']
    sc = SparkContext(appName="PythonWordCount")
    wifiacc = sc.textFile("/user/omnilab/warehouse/sjtu_wifi_users", 1).map(lambda x:(x.split("|")[0],x))
    # wifiinfo= sc.textFile("/user/omnilab/warehouse/sjtu_wifi_syslog_clean/wifilog2014-09.clean",1)\
    #         .map(lambda x:(x.split(",")[5],timeconverter(x)))\
    #         .join(wifiacc)\
    #         .map(lambda x: x[1][1].split("|")[1]+"|"+x[1][0])
    wifiinfo= sc.textFile("/user/omnilab/warehouse/sjtu_wifi_syslog_clean/wifilog2014-09.clean",1)\
            .map(lambda x:(x.split(",")[5],x.split(",")[3]))\
            .join(wifiacc)\
            .map(lambda x: x[1][0])
            # .map(lambda x: x[1][1].split("|")[1]+"|"+x[1][0])
    for i in range(10,19):
        if i>12:
            wifiinfo2=sc.textFile("/user/omnilab/warehouse/sjtu_wifi_syslog_clean/wifilog2015-0"+str(i-12)+".clean",1)
        else:
            wifiinfo2=sc.textFile("/user/omnilab/warehouse/sjtu_wifi_syslog_clean/wifilog2014-"+str(i)+".clean",1)
        wifiinfo2=wifiinfo2.map(lambda x:(x.split(",")[5],x.split(",")[3]))\
                            .join(wifiacc)\
                            .map(lambda x: x[1][0])
                            # .map(lambda x: x[1][1].split("|")[1]+"|"+x[1][0])
        wifiinfo=wifiinfo.union(wifiinfo2)
    # now the format is (student number|format time|weekday number)
    bn=wifiinfo.distinct().count()
    wifiinfo=wifiinfo.distinct()
    # wifiinfo=wifiinfo.map(lambda x: (x.split("|")[0]+x.split("|")[1][:13],x)).reduceByKey(lambda a,b:b).map(lambda x: x[1])\
    #             .map(lambda x: (x.split("|")[1].split(" ")[1][:2],x))
    # week=wifiinfo.filter(lambda x: int(x[1].split("|")[2])<6 and (x[1].split("|")[1].split(" ")[0] not in holiday))
    # weekend=wifiinfo.filter(lambda x: int(x[1].split("|")[2])>=6 and (x[1].split("|")[1].split(" ")[0] not in holiday))
    # wn=week.map(lambda x:(x[1].split("|")[1].split(" ")[0],1)).distinct().count()
    # wen=weekend.map(lambda x:(x[1].split("|")[1].split(" ")[0],1)).distinct().count()
    # bn=week.countByKey()
    # cn=weekend.countByKey()
    wifiinfo.coalesce(1).saveAsTextFile(sys.argv[1])
    print("******************OK***********************"+str(bn))
    sc.stop()