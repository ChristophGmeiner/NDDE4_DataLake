# ETL Process for Sparkify DB using Apache Spark

This is my fourth project for the Udacity Nanodegree of Data Engineering. It is about an etl process (Spark and AWS S3 based) for Sparkify.

Sparkify is a simulated (not-real) online music streaming service.

This Git repository shows how to script an etl process for loading data from json raw data (stored in an AWS S3 bucket), for creating fact and dimension tables from these files by transforming them using Apache Spark and save them back to the S3 bucket as csv files Basically it is a little bit similar to the third NDDE project (see here: https://github.com/ChristophGmeiner/NDDE3_DataWarehouse_AWS)

This is done using Python (specifically the pyspark package). Data checks on boger datasets were processed on an AWS EMR cluster.

## Purpose of the Database sparkifydb

The sparkifydb is a data kake stored in an AWS S3 bucket and is about storing information about songs and listening behaviour of the users.

The analytical goal of this database to get all kinds of insights into the user beahviour (listenting preferences, highest rated artist, high volume listening times, etc.)

Please be aware that this data is for demonstration purposes only and therefore not very complete i.e. we only see some users and only data for one month, i.e. Nov. 2018.

## Description of the ETL Pipeline

All confidential information needed for connecting to AWS is stored in a local file (not part of this repo), i.e. dl.cfg. See the scripts for details on that.

The ETL process is done in a Apache Spark cluster using the pyspark package. Raw json data is stored in an AWS S3 bucket (owned by udacity). The json data is loaded into Spark dataframes, transformed to star-schema dataframes and afterwards written to parquet files in another AWS S3 bucket for further and later analysis.

The data is the same as in my NDDE3 project. See details here: https://github.com/ChristophGmeiner/NDDE3_DataWarehouse_AWS

Please find the description of each tabke / dataframe object below:

### songplays_table
This object is the fact table. Since it would not make any sense to accept NULL values for either song or artist in this table (for analytical reasons), this table is pretty small. This is due to the raw data restrictions mentioned in the purpose section above. Details on that can also be seen in the DataChecks notebook.

### user_table
This table shows masterdata about users. Basically the user_id shoud be the unique key in this dataframe, but since many users probablky start as free user and become paid users after that, the unique ID in the current setup has to be extended to the level field as well.

### songs_table
This table shows masterdata about songs.

### artists_table
This table shows masterdata about artists. 

### time_table
This table shows time and date based masterdata for each timestamp a songplay took place. 


## Scripts and files

### etl.py
Production-ready etl python script, which takes all the etl steps mentioned above.

### etl,ipynb
Explains the steps in etl.py

### s3_inspect.ipynb
Demo notebook for examining the contents of an AWS S3 bucket.

### RunScripts.sh
Production ready bash script, which installs a necessary AWS SDK python package (which is apparently not installed by default on Udacity VM) first and then runs the etl.py script.
On another machines these step is not necessary. Running only the python script is sufficient for this matter.

### Data_Checks.ipynb
This notebook shows some data checks and some basic approaches for data analysis with the newly generated data. These checks are similar to the Check notebook, I created in the NDDE3 project (find link for that repo above). 