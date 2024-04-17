#!/bin/bash 

echo "Running HDFS operations"

./HDFS_operations.sh

echo "Execution of HDFS operations completed"

#specifying paths to scripts 

mapreduce_jar="/root/Desktop/big-data-analytics-rwc/mapreduce/RetailSalesAnalysis.jar"

hive_script="/root/Desktop/big-data-analytics-rwc/hive/WorldOneCensusUsingHive.hql"

pig_script="/root/Desktop/big-data-analytics-rwc/Pig/Digi_Analysis_using_Pig.pig"

echo "running mapreduce jobs"

hadoop jar "$mapreduce_jar"

echo "execution of mapreduce jobs completed"

echo "running hive scripts"

hive -f "$hive_script"

echo "hive script execution completed"

echo "running pig scripts"

pig -x mapreduce "$pig_script"

echo "pig script exection completed"

 