#! /bin/bash
# Author: Basu,Sanjoy

# Copying file to hdfs

loc="hdfs://labcluster1-m/data/denverCrime/crime.csv"

hdfs dfs -rm $loc 1>&2

if [ -f ./crime.csv ]
then
   hdfs dfs -put crime.csv $loc 1>&2

else
   echo "data file not found"
fi
