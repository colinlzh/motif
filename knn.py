from numpy import *

import operator
import os

def createDataSet():
    group=array([[1.0,1.1],[1.0,1.0],[0,0],[0,0.1]])
    labels=['A','A','B','B']
    return group,labels
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
        	classCount[label]=classCount.get(label,0)+1
        sortedcount=sorted(classCount.items(), key=lambda d:d[1], reverse=True)
        return sortedcount[0][0]
def file2matrix(filename):
    fr=open(filename)
    arrayline=fr.readlines()
    nline=len(arrayline)
    mat= zeros((nline,16))
    labelvector=[]
    index=0
    for line in arrayline:
        line=line.strip()
        lis=line.split("|")
        mat[index,:]=lis[:16]
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
def test(qq):
    m,l=file2matrix("gradesb")
    n,r,minv=norm(m)
    testvector=int(n.shape[0]*0.10)
    e=0.0
    for i in range(testvector):
        result= classify0(n[i,:],n[testvector:n.shape[0],:],\
                          l[testvector:n.shape[0]],qq)
        if (result!=l[i]): e +=1.0
    print(e/float(testvector))
def img2vector(filename):
    returnvect=zeros((1,1024))
    fr=open(filename)
    for i in range(32):
        line=fr.readline()
        for j in range(32):
            returnvect[0,32*i+j]=int(line[j])
    return returnvect
def hand():
    l=[]
    train=os.listdir("trainingDigits")
    m=len(train)
    trainmat=zeros((m,1024))
    for i in range(m):
        namestr=train[i]
        name=namestr.split(".")[0]
        l.append(int(name.split("_")[0]))
        trainmat[i,:]=img2vector("trainingDigits/%s" % namestr)
    test=os.listdir("testDigits")
    err=0.0
    n=len(test)
    testmat=zeros((n,1024))
    for i in range(n):
        namestr=test[i]
        name=namestr.split(".")[0]
        testmat=img2vector("testDigits/%s" % namestr)
        r=classify0 (testmat,trainmat,l,3)
        if(r is not int(name.split("_")[0])): err+=1.0
    print(err/n)
if __name__ == "__main__":
    for k in range(2,10):
        test(k)
