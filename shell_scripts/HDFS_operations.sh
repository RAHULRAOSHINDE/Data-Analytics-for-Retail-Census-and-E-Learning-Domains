#!/bin/bash

#creating a new directory retails_analysis in HDFS

hadoop fs -mkdir /user/retail_analysis

#creating a new directory WorldOne in HDFS

hadoop fs -mkdir /user/WorldOne

hadoop fs -mkdir /user/hive_output

hadoop fs -mkdir /user/pig_output

#creating a new directory Digi_Analysis in HDFS

hadoop fs -mkdir /user/Digi_Analysis

#copying files from local file system to retail_analysis in HDFS

hadoop fs -copyFromLocal  /root/Desktop/big-data-analytics-rwc/Dataset/RetailData.txt  /user/retail_analysis

hadoop fs -copyFromLocal /root/Desktop/big-data-analytics-rwc/Dataset/CustomerAddress.txt /user/retail_analysis

#copying files from local file system to Census in HDFS

hadoop fs -copyFromLocal /root/Desktop/big-data-analytics-rwc/Dataset/Census.tsv /user/WorldOne

#copying files from local file system to Digi_Analysis in HDFS

hadoop fs -copyFromLocal /root/Desktop/big-data-analytics-rwc/Dataset/Participants.csv /user/Digi_Analysis

hadoop fs -copyFromLocal /root/Desktop/big-data-analytics-rwc/Dataset/Courses.csv /user/Digi_Analysis