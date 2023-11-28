Introduction

This project is a music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The Task is to build an ETL Pipeline that extracts their data from S3, staging it in Redshift and then transforming data into a set of Dimensional and Fact Tables for their Analytics Team to continue finding Insights to what songs their users are listening to.

Description

Application of Data warehouse and AWS to build an ETL Pipeline for a database hosted on Redshift Will need to load data from S3 to staging tables on Redshift and execute SQL Statements that create fact and dimension tables from these staging tables to create analytics

Project Datasets

Song Data Path --> s3://udacity-dend/song_data 
Log Data Path --> s3://udacity-dend/log_data 
Log Data JSON Path --> s3://udacity-dend/log_json_path.json

Song Dataset

The first dataset is a subset of real data from the Million Song Dataset:

song_data/A/B/C/TRABCEI128F424C983.json 
song_data/A/A/B/TRAABJL12903CDCF1A.json



Log Dataset

The second dataset consists of log files in JSON format. The log files in the dataset with are partitioned by year and month.
example:
log_data/2018/11/2018-11-12-events.json log_data/2018/11/2018-11-13-events.json
And below is an example of what a single log file, 2018-11-13-events.json, looks like.



The database contains: Fact table:

A Star Schema would be required for optimized queries on song play queries

Fact Table

songplays - user songplays and contains foreign keys of related dimension tables.

Dimension Tables

users - users in the app user_id, first_name, last_name, gender, level

songs - songs in music database song_id, title, artist_id, year, duration

artists - artists in music database artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

Project Template

Project Template include four files:

1. create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.

2. etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

3. sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.

4. README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

Create Table Schema

Write a SQL CREATE statement for each of these tables in sql_queries.py
Complete the logic in create_tables.py to connect to the database and create these tables
Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
Launch a redshift cluster and create an IAM role that has read access to S3.
Add redshift database and IAM role info to dwh.cfg.
Test by running create_tables.py and checking the table schemas in your redshift database.
Build ETL Pipeline

Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
Delete your redshift cluster when finished.
Final Instructions

Import all the necessary libraries

Write the configuration of AWS Cluster, store the important parameter in some other file
Configuration of boto3 which is an AWS SDK for Python
Using the bucket, can check whether files log files and song data files are present
Create an IAM User Role, Assign appropriate permissions and create the Redshift Cluster
Get the Value of Endpoint and Role for put into main configuration file
Authorize Security Access Group to Default TCP/IP Address
Launch database connectivity configuration
Go to Terminal run the following command "python create_tables.py" and then "etl.py"
