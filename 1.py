#coding:utf-8  
import sys  
reload(sys)  
sys.setdefaultencoding("utf8")
from pyspark import SparkContext

if __name__ == "__main__":     
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <file>"
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")   
    alluser = sc.textFile("./NET/wifiuserfilt3/", 1)
    a=alluser.map(lambda x: x.split(",")[6].split(" ")[0]).distinct().sortBy(lambda x:x,ascending=False).collect()
    print("******************OK***********************"+a[0])
    sc.stop()