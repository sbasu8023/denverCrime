#! /bin/bash 

# Author: Basu, Sanjoy
# Pull csv file from denver.gov

dFile="crime.csv"
tstamp=`date +%Y%m%d%H%M`
url="https://www.denvergov.org/media/gis/DataCatalog/crime/csv/crime.csv"

# Checking and backing up files

if [ -f crime.csv ]
then
  echo "Backing up old data file"
  mv ./$dFile ./bkup/$dFile$tstamp
fi

#Downloading data file
echo "$tstamp fetching data ..."
wget $url 1>&2 



# Printing status
if [ -f crime.csv ]
then
   echo "Data fetch successful"
else
   echo "Data fetch failed" 
fi
