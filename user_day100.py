# -*- coding:utf-8 -*- ＃
import sys
import time
from operator import add
from pyspark import SparkContext

def key(x): 
    t=x.split(",")
    return (t[0]+","+t[6].split(" ")[0],1)
# this code aim to filt people who living in school less than 70days per school term,and also who
# have less than three stay points per day(which means this guy do not use SJTU too much)
if __name__ == "__main__":
     
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <file>"
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    al = sc.textFile("./NEWNET/tradefilt", 1)
    source=al.map(lambda x:(x.split(",")[0],x))
    user=al.map(lambda x:key(x))\
            .reduceByKey(lambda a,b:b)\
            .map(lambda x: (x[0].split(",")[0],1))\
            .reduceByKey(add)\
            .filter(lambda x: x[1]>=70)\
            .map(lambda x: (x[0].split(",")[0],x[1]))
    action=al.map(lambda x:(x.split(",")[0],1)).reduceByKey(add)
    average=user.join(action).filter(lambda x:x[1][1]/x[1][0]>=4)
    al=source.join(average)\
            .sortByKey()\
            .map(lambda x: x[1][0])
    al.saveAsTextFile(sys.argv[1])
    print("******************OK***********************")
    sc.stop()