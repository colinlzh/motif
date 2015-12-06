# -*- coding:utf-8 -*- ＃
import sys
import time
import datetime
from operator import add
from pyspark import SparkContext

def CompareTime(s):
    d =s.split("\t")[3]
    time='2014-11-00'
    time1='2014-12-00'
    if (d > (time)) and d<time1: return True
    return False

def func(s):
	if s.find(u"男")!=-1 :return "1"
	return '0'

if __name__ == "__main__":
     
    sc = SparkContext(appName="PythonWordCount")
    trade = sc.textFile("./EMC/trade.txt", 1)\
    			.map(lambda x:(x.split("\t")[0],x))
    account=sc.textFile("./EMC/account.txt",1)\
    			.map(lambda x:(x.split("\t")[0],x.split("\t")[1]))
    trade=trade.join(account).map(lambda x:(x[1][0].split("\t")[1],x[1][1]+","+x[1][0].split("\t")[3]))
    place=sc.textFile("./EMC/merchant.txt").map(lambda x:(x.split("\t")[2],x.split("\t")[1]))
    trade=trade.join(place).map(lambda x: x[1][0]+"\t"+x[1][1])	
    trade.saveAsTextFile("./NET/trade")
    print("******************OK***********************")
    sc.stop()