#! /bin/bash
# Author : Basu,Sanjoy

tstamp=`date +%Y%m%d%H%M`
fmat=".csv"
if [ -d denverCrime ]
then
   rm -r -f ./denverCrime
fi


# copying all directories from hdfs

hdfs  dfs -get  hdfs://labcluster1-m/data/denverCrime/

rm -f ./denverCrime/crime.csv

for dr in `ls ./denverCrime/`
do 
  for fl in `ls ./denverCrime/$dr/`
  do
    cat "./denverCrime/$dr/$fl" >> $dr$tstamp$fmat
  done
done
