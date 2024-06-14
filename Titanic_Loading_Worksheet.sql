// Creating Objects 
CREATE OR REPLACE DATABASE TITANIC_ETL;
USE DATABASE TITANIC_ETL;
USE SCHEMA PUBLIC;

//creating file_format
CREATE OR REPLACE FILE FORMAT CSV_FF
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

//Create Internal Stage to load the data
CREATE OR REPLACE STAGE RAW_DATA_STAGE
FILE_FORMAT = 'CSV_FF';

CREATE OR REPLACE STAGE RAW_DATA_STAGE2;
 

//creating Titanic_raw_dataset table and copying data
Create or replace table Titanic_raw_dataset(PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked)
as
select $1, $2, $3,$4,$5,$6,$7,$8, $9,$10, $11, $12 from @raw_data_stage/Titanic-Dataset.csv (file_format => 'CSV_FF') ;

Select * from Titanic_raw_dataset limit 100;

 
