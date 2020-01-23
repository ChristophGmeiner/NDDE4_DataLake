# ETL Process for Sparkify DB using Apache Spark

This is my fourth project for the Udacity Nanodegree of Data Engineering. It is about an etl process (Spark and AWS S3 based) for Sparkify.

Sparkify is a simulated (not-real) online music streaming service.

This Git repository shows how to script an etl process for loading data from json raw data (stored in an AWS S3 bucket), for creating fact and dimension tables from these files by transforming them using Apache Spark and save them back to the S3 bucket as csv files Basically it is a little bit similar to the third NDDE project (see here: https://github.com/ChristophGmeiner/NDDE3_DataWarehouse_AWS)

This is done using Python (specifically the pyspark package).

## Purpose of the Database sparkifydb

The sparkifydb is a data kake stored in an AWS S3 bucket and is about storing information about songs and listening behaviour of the users.

The analytical goal of this database to get all kinds of insights into the user beahviour (listenting preferences, highest rated artist, high volume listening times, etc.)

Please be aware that this data is for demonstration purposes only and therefore not very complete i.e. we only see some users and only data for one month, i.e. Nov. 2018.

## Description of the ETL Pipeline

All confidential information needed for connecting to AWS is stored in a local file (not part of this repo), i.e. dl.cfg. See the scripts for details on that.