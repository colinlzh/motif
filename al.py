# -*- coding:utf-8 -*- ＃
import sys
import time
from operator import add
from pyspark import SparkContext
from numpy import *
import operator
import os

def first(x):
    t=x.split(",")
    return t[0]+","+t[1]+","+t[2]+","+t[3]+","+t[4]+","+t[5]+","+t[6]

def location_to_motif(locations):
    dateFlag = ''
    motifList = []
    for i in xrange(0,len(locations)):
        t=locations[i].split(" ")
        _date = t[1]
        _time = t[2]
        station = t[0]
        if dateFlag != _date:#if another day, initialize
            motifList.append({})
            nodeDict = {}
        if not nodeDict.has_key(station):#map station name to node num
            nodeDict[station] = len(nodeDict)
        if not motifList[-1].has_key(nodeDict[station]):#add the appeared node to today's motif graph
            motifList[-1][nodeDict[station]] = []
        if (i+1)<len(locations):
            if locations[i+1].split(' ')[1]==_date:#yet is today's
                n_station = locations[i+1].split(' ')[0]
                if not n_station==station:
                    if not nodeDict.has_key(n_station):
                        nodeDict[n_station] = len(nodeDict)
                    if not motifList[-1][nodeDict[station]].count(nodeDict[n_station])>0:
                        motifList[-1][nodeDict[station]].append(nodeDict[n_station])
        dateFlag = _date

    return motifList
def style_conv(list_dict):
    dicts = str(list_dict).strip('[]')
    dicts = dicts.split(', {')
    rsl = []
    for i in xrange(0, len(dicts)):
        if i ==0:
            rsl.append(dicts[i])
        else:
            rsl.append('{' + dicts[i])
    return rsl
def converter(lis):
    lis.sort(key=lambda x:int(x.split("|")[1]))
    d=1
    a=""
    for i in lis:
        if i.split("|")[1]==str(d):
            a=a+i.split("|")[0]+"|"
            d=d+1
        else :
            while(d<int(i.split("|")[1])):
                a=a+"0"+"|"
                d=d+1
            a=a+i.split("|")[0]+"|"
            d=d+1
    while(d<=10):
        a=a+"0"+"|"
        d=d+1
    return a
def word2num(w):
    if w==u"博士":
        return "3"
    if w==u"硕士":
        return "2"
    if w==u"本科":
        return "1"
    if w==u"女":
        return "1"
    if w==u"男":
        return "2"
    else :
        return "0"
def classify0(inx,dataset,labels,k):
        datasetsize=dataset.shape[0]
        diffmat=tile(inx,(datasetsize,1))-dataset
        sqdiffmat=diffmat**2
        sqdistace=sqdiffmat.sum(axis=1)
        distance=sqdistace**0.5
        sorte=distance.argsort()
        classCount={}
        for i in range(k):
            label=labels[sorte[i]]
            classCount[label]=classCount.get(label                         ,0)+1
        sortedcount=sorted(classCount.items(), key=lambda d:d[1], reverse=True)
        return sortedcount[0][0]
def file2matrix(nline,b):
    mat= zeros((nline,20))
    labelvector=[]
    index=0                                       
    for line in b:
        line=line.strip()
        lis=line.split("|")
        mat[index,:]=lis[:20]
        labelvector.append(int(lis[-1]))
        index+=1
    return mat,labelvector

def norm(data):
    minv=data.min(0)
    maxv=data.max(0)
    ran=maxv-minv
    normdata=zeros(shape(data))
    m=data.shape[0]
    normdata=(data-tile(minv,(m,1)))/tile(ran,(m,1))
    return normdata,ran,minv
def test(qq,a,b):
    m,l=file2matrix(a,b)
    n,r,minv=norm(m)
    testvector=int(n.shape[0]*0.10)
    e=0.0
    for i in range(testvector):
        result= classify0(n[i,:],n[testvector:n.shape[0],:],\
                          l[testvector:n.shape[0]],qq)
        if (result!=l[i]): e +=1.0
    print(e/float(testvector))
def minus(x):
    t1=x[1][1].split("|")
    t2=x[1][0].split("|")
    t3=""
    for i in range(10):
        t3=t3+(str(float(t1[i])-float(t2[i])))+"|"
    return t3+t1[10]

if __name__ == "__main__":
    holiday=['2014-10-01', '2014-10-02', '2014-10-03', '2015-01-01', '2015-04-05', '2015-05-01', '2015-01-19', '2015-01-20', '2015-01-21', '2015-01-22', '2015-01-23', '2015-01-24', '2015-01-25', '2015-01-26', '2015-01-27', '2015-01-28', '2015-01-29', '2015-01-30', '2015-01-31', '2015-02-01', '2015-02-02', '2015-02-03', '2015-02-04', '2015-02-05', '2015-02-06', '2015-02-07', '2015-02-08', '2015-02-09', '2015-02-10', '2015-02-11', '2015-02-12', '2015-02-13', '2015-02-14', '2015-02-15', '2015-02-16', '2015-02-17', '2015-02-18', '2015-02-19', '2015-02-20', '2015-02-21', '2015-02-22', '2015-02-23', '2015-02-24', '2015-02-25', '2015-02-26', '2015-02-27', '2015-02-28'] 
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile("./NET/wifiuserfilt3", 1)
    boy=lines.filter(lambda x:x.find("null")==-1 and time.strptime (x.split(",")[-1],"%Y-%m-%d %H:%M:%S").tm_wday<5 and x.split(",")[-1].split(" ")[0] not in holiday)
    boyf=boy.filter(lambda x:x.split(",")[-1].split(" ")[0]<"2015-01-00")\
                .map(lambda x:x.split(",")[0]+"\t"+x.split(",")[5]+" "+x.split(",")[6])\
                .sortBy(lambda x: x.split("\t")[0]+x.split(" ")[1])\
                .map(lambda x:(x.split("\t")[0],x.split("\t")[1]))\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda x:(x[0],style_conv(location_to_motif(x[1]))))\
                .flatMapValues(lambda x:x)
    boyl=boy.filter(lambda x:x.split(",")[-1].split(" ")[0]>="2014-01-00")\
                .map(lambda x:x.split(",")[0]+"\t"+x.split(",")[5]+" "+x.split(",")[6])\
                .sortBy(lambda x: x.split("\t")[0]+x.split(" ")[1])\
                .map(lambda x:(x.split("\t")[0],x.split("\t")[1]))\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda x:(x[0],style_conv(location_to_motif(x[1]))))\
                .flatMapValues(lambda x:x)
                # .map(lambda x:x[0]+"\t"+x[1])
    # boy.saveAsTextFile("./NET/motif")
    # boyc=boy.count()  #key is usernumber
    # boy=boy.map(lambda x: (x[1],1)).reduceByKey(add).filter(lambda x: x[1]>100)\
    #             .sortBy(lambda x:x[1],ascending=False)\
    #             .map(lambda x: x[0]+"|"+str(x[1])+"|"+str(float(x[1])/float(boyc)))
    # boy=sc.textFile("./NET/motif",1).map(lambda x:(x.split("\t")[0],x.split("\t")[1]))
    boycf=boyf.map(lambda x:(x[0],1)).reduceByKey(add)  #key is usernumber
    boycl=boyl.map(lambda x:(x[0],1)).reduceByKey(add)  #key is usernumber
    motif=sc.textFile("./NET/wifial10",1).map(lambda x:(x.split("|")[0],x.split("|")[1]))
    info=sc.textFile("./EMC/account.txt",1).map(lambda x:(x.split("\t")[1],(word2num(x.split("\t")[5]))))
    #next key is usernum+motif cat
    boyf=boyf.map(lambda x: (x[0]+"|"+x[1],1))\
                .reduceByKey(add)\
                .map(lambda x: (x[0].split("|")[0],x[0]+"|"+ str(x[1])))\
                .join(boycf)\
                .map(lambda x:(x[1][0].split("|")[1],x[0]+"|"+x[1][0].split("|")[1]+"|"+str(float(x[1][0].split("|")[2])/(x[1][1]))))\
                .join(motif)\
                .map(lambda x:(x[1][0].split("|")[0],x[1][0].split("|")[2]+"|"+x[1][1]))\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda x:(x[0],converter(x[1])))\
                .join(info)\
                .map(lambda x:(x[0],x[1][0]+x[1][1]))
    boyl=boyl.map(lambda x: (x[0]+"|"+x[1],1))\
                .reduceByKey(add)\
                .map(lambda x: (x[0].split("|")[0],x[0]+"|"+ str(x[1])))\
                .join(boycl)\
                .map(lambda x:(x[1][0].split("|")[1],x[0]+"|"+x[1][0].split("|")[1]+"|"+str(float(x[1][0].split("|")[2])/(x[1][1]))))\
                .join(motif)\
                .map(lambda x:(x[1][0].split("|")[0],x[1][0].split("|")[2]+"|"+x[1][1]))\
                .groupByKey()\
                .mapValues(list)\
                .map(lambda x:(x[0],converter(x[1])))\
                .join(info)\
                .map(lambda x:(x[0],x[1][0]))
    boy=boyl.join(boyf).map(minus)
    boy.coalesce(1).saveAsTextFile(sys.argv[1])
    a=boy.count()
    b=boy.collect()
    for k in range(2,10):
        test(k,a,b)
        print(k)
    print("******************OK***********************"+str(boyc))
    sc.stop()