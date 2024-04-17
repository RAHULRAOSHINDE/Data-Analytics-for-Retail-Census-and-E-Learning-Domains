--Creating a Database as CensusDB

CREATE DATABASE IF NOT EXISTS CensusDB
COMMENT 'World One Census Bureau'
WITH DBPROPERTIES('creator' = 'rahul')
;

USE  CensusDB;

--Creating a  table as  Census

CREATE TABLE IF NOT EXISTS Census (
DisplayID STRING,
EmploymentType STRING,
EduQualification STRING,
MartialStatus STRING,
JobType STRING,
WorkingHoursPerWeek INT,
Country STRING,
Salary STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('creator' = 'rahul')
;

--Populating  the Census.tsv file into a table Census

LOAD DATA LOCAL INPATH '/root/Desktop/big-data-analytics-rwc/Dataset/Census.tsv' INTO TABLE Census;

--Creating  two views out of the table Census  as USCensus_View and OthersCensus_View 

CREATE VIEW USCensus_View AS 
SELECT * FROM Census WHERE Country = 'US'
;

CREATE VIEW OtherCensus_View AS
SELECT * FROM Census WHERE Country != 'US';

-- To  compute the total count of people under each marital status

INSERT OVERWRITE DIRECTORY '/user/hive_output/peoplecount_martialstatus.csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
SELECT  martialstatus, COUNT(DisplayID) AS total_count
FROM Census 
GROUP BY martialstatus;

-- To compute the average working hours based on marital status

INSERT OVERWRITE DIRECTORY '/user/hive_output/workinghours_martialstatus.csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
SELECT martialstatus,ROUND(AVG(WorkingHoursPerWeek),2)  AS avg_working_hours
FROM Census
GROUP BY martialstatus;

--To compute total number of people in each job type

INSERT OVERWRITE DIRECTORY '/user/hive_output/peoplecount_eachjob.csv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
SELECT jobtype,COUNT(DisplayID) AS total_people
FROM Census
GROUP BY jobtype;

--To compute the average working hours based on salary range

INSERT OVERWRITE DIRECTORY '/user/hive_output/workinghours_salaryrange.csv' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
SELECT salary AS salary_range , ROUND(AVG(WorkingHoursPerWeek),2) AS avg_working_hours
FROM Census
GROUP BY salary;

--To compute total number of people with salary less than 50K and total number of people with salary greater than 50K

INSERT OVERWRITE DIRECTORY '/user/hive_output/salary.csv' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
SELECT SUM(IF(salary = '<=50K',1,0)) AS salary_below_50K,SUM(IF(salary = '>50K',1,0)) AS salary_above_50K
FROM census;

--To create a static partition table named census_ETP_sp with partition on column EmploymentType value Private

CREATE TABLE  IF NOT EXISTS census_ETP_sp(
DisplayID INT,
EduQualification STRING,
MartialStatus STRING,
JobType STRING,
WorkingHoursPerWeek INT,
Country STRING,
Salary STRING
)
PARTITIONED BY (EmploymentType STRING) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('creator' = 'rahul')
;


INSERT OVERWRITE TABLE census_etp_sp 
PARTITION(EmploymentType)
SELECT DisplayID,EduQualification,MartialStatus,JobType,WorkingHoursPerWeek,Country,Salary,EmploymentType 
FROM census
WHERE EmploymentType = 'Private'
;


set hive.exec.dynamic.partition.mode=nonstrict 

--To create a dynamic partition table named census_MS_dp with partition on column MaritalStatus

CREATE TABLE  IF NOT EXISTS census_MS_dp(
DisplayID INT,
EmploymentType STRING,
EduQualification STRING,
JobType STRING,
WorkingHoursPerWeek INT,
Country STRING,
Salary STRING
)
PARTITIONED BY (MartialStatus STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('creator' = 'rahul')
;

INSERT OVERWRITE TABLE census_MS_dp 
PARTITION(MartialStatus)
SELECT DisplayID,EmploymentType,EduQualification,JobType,WorkingHoursPerWeek,Country,Salary,MartialStatus FROM census;

--To create a bucketed table named census_ET_bucket using the column employmentType.

CREATE TABLE IF NOT EXISTS census_ET_bucket(
DisplayID INT,
EmploymentType STRING,
EduQualification STRING,
MartialStatus STRING,
JobType STRING,
WorkingHoursPerWeek INT,
Country STRING,
Salary STRING
)
CLUSTERED BY  (EmploymentType) INTO 5 buckets
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('creator'= 'rahul')
;

INSERT OVERWRITE TABLE census_ET_bucket 
SELECT * FROM census;

--To compute average working hours for every bucket

SELECT employmenttype,AVG(workinghoursperweek)
FROM census_ET_bucket
GROUP BY employmenttype;

--To create a UDF which decides the Incometax slab of census data based on the EmploymentType.

ADD JAR /root/Desktop/big-data-analytics-rwc/hive/IncomeTaxUDF.jar;

--creating a temporary function for the UDF 

CREATE TEMPORARY FUNCTION IncomeTaxCalculator AS 'com.incometax.calculator.IncomeTaxCalculator';
 
INSERT OVERWRITE DIRECTORY '/user/hive_output/incometax.csv' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY  ','
SELECT DisplayID,EmploymentType,EduQualification,MartialStatus,JobType,WorkingHoursPerWeek,Country,Salary,IncomeTaxCalculator(EmploymentType) FROM Census;




