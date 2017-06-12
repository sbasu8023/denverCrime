# -*- coding: utf-8 -*-
"""
# ETL crime data

@author: sanjoybasu
"""

from pyspark import SparkContext
from time import time

sc=SparkContext()
tstamp=str(time())

#  Save location on HDFS

hdfsLoc="hdfs://labcluster1-m/data/denverCrime/"
criminalGeoPth=hdfsLoc+"criminalGeo"+tstamp
trafficGeoPth=hdfsLoc+"trafficGeo"+tstamp
trafficTypeCntPth=hdfsLoc+"trafficTypeCnt"+tstamp
trafficNbhCntPth=hdfsLoc+"trafficNbhCnt"+tstamp
criminalNbhCntPth=hdfsLoc+"criminalNbhCnt"+tstamp
criminalTypeCntPth=hdfsLoc+"trafficNbhCnt"+tstamp


# Creating initial Rdd
denCrimeRdd=sc.textFile("hdfs://labcluster1-m/data/denverCrime/crime.csv")

#### Traffic ###
trafficRdd=denCrimeRdd.map(lambda ln: ln.split(',')).filter(lambda fl:fl[18]=="1")
trafficGeo=trafficRdd.map(lambda fl: (str([fl[13],fl[12]]), 1)).reduceByKey(lambda v1,v2: v1+v2).map(lambda (a, b): (b, a)).sortByKey(ascending=False)
trafficTypeCnt=trafficRdd.map(lambda fl: (fl[4],1)).reduceByKey(lambda v1,v2: v1+v2).map(lambda (a,b) : (b,a)).sortByKey(ascending=False)
trafficNbhCnt=trafficRdd.map(lambda fl: (fl[16],1)).reduceByKey(lambda v1,v2: v1+v2).map(lambda (a,b) : (b,a)).sortByKey(ascending=False)


#### Criminal ###
criminalRdd=denCrimeRdd.map(lambda ln: ln.split(',')).filter(lambda fl:fl[17]=="1")
criminalGeo=criminalRdd.map(lambda fl: (str([fl[13],fl[12]]),1)).reduceByKey(lambda v1,v2: v1+v2).map(lambda (a, b): (b, a)).sortByKey(ascending=False)
criminalTypeCnt=criminalRdd.map(lambda fl: (fl[4],1)).reduceByKey(lambda v1,v2: v1+v2).map(lambda (a,b) : (b,a)).sortByKey(ascending=False)
criminalNghCnt=criminalRdd.map(lambda fl: (fl[16],1)).reduceByKey(lambda v1,v2: v1+v2).map(lambda (a,b) : (b,a)).sortByKey(ascending=False)



#Saving file
trafficGeo.saveAsTextFile(trafficGeoPth)
trafficTypeCnt.saveAsTextFile(trafficTypeCntPth)
trafficNbhCnt.saveAsTextFile(trafficNbhCntPth)
criminalGeo.saveAsTextFile(criminalGeoPth)
criminalTypeCnt.saveAsTextFile(criminalTypeCntPth)
criminalNghCnt.saveAsTextFile(criminalNbhCntPth)