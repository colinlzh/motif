# -*- coding:utf-8 -*-
import sys
import time
from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    sc = SparkContext(appName="PythonWordCount")
    al = sc.textFile("./taxi_zip/*/*/*", 1)
    al = al.sortBy(lambda x:x.split("|")[0]+x.split("|")[9])
    al.saveAsTextFile(sys.argv[1])
    sc.stop()