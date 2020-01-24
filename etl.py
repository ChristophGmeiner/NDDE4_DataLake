import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import boto3

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''Function for loading songs and artist data
    Parameters:
        spark: SparkSession,
        input_data: path of the input data. The path has to be a S3 bucket. 
                    Please think about putting an "a" after s3, otherwise spark
                    won't recognize the S3 path
        output_data: path of the output data.The path has to be a S3 bucket. 
                    Please think about putting an "a" after s3, otherwise spark
                    won't recognize the S3 path
    '''
    
    bucketname = input_data[6: ]
    bucketname = bucketname[0:bucketname.find("/")]
    
    lcobj = list(s3.list_objects_v2(Bucket=bucketname, 
                                Prefix="song_data/").values())
    
    song_data = []
    for k in lcobj[2]:
        if k["Key"].find(".json") > -1:
            song_data.append("s3a://" + bucketname + "/" + k["Key"])
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", 
                              "duration").distinct()\
                    .orderBy(F.col("song_id"))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite")\
                 .parquet(output_data + "song_table.parquet/")

    # extract columns to create artists table
    artists_table = df.select("artist_id", 
                               F.col("artist_name").alias("name"),
                               F.col("artist_location").alias("location"),
                               F.col("artist_latitude").alias("latitude"),
                               F.col("artist_longitude").alias("longitude"))\
                       .distinct().orderBy(F.col("artist_id"))
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
                 .parquet(output_data + "artist_table.parquet/")


def process_log_data(spark, input_data, output_data):
    '''Function for loading log data
       Parameters:
        spark: SparkSession,
        input_data: path of the input data. The path has to be a S3 bucket. 
                    Please think about putting an "a" after s3, otherwise spark
                    won't recognize the S3 path
        output_data: path of the output data.The path has to be a S3 bucket. 
                    Please think about putting an "a" after s3, otherwise spark
                    won't recognize the S3 path
    '''
    
    bucketname = input_data[6: ]
    bucketname = bucketname[0:bucketname.find("/")]
    
    lcobj = list(s3.list_objects_v2(Bucket=bucketname, 
                                Prefix="log_data/").values())
    
    log_data = []
    for k in lcobj[2]:
        if k["Key"].find(".json") > -1:
            log_data.append("s3a://" + bucketname + "/" +k["Key"])
            
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(F.col("page")=="NextSong")

    # extract columns for users table    
    user_table = df.select(F.col("userId").alias("user_id"), 
                           F.col("firstname").alias("first_name"), 
                           F.col("lastname").alias("last_name"), 
                           "gender", "level").distinct()\
                   .orderBy("userId")
    
    # write users table to parquet files
    user_table.write.mode("overwrite")\
                 .parquet(output_data + "user_table.parquet/")
    
    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", F.expr("cast(ts / 1000 as timestamp)"))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime", F.expr("cast(timestamp as date)"))
    
    # extract columns to create time table
    df.registerTempTable("dftab")
    time_table = spark.sql("""
                            SELECT DISTINCT
                            timestamp AS start_time,
                            HOUR(timestamp) AS hour,
                            DAY(timestamp) AS day,
                            WEEKOFYEAR(timestamp) as week,
                            MONTH(timestamp) as month,
                            YEAR(timestamp) as year,
                            DAYOFWEEK(timestamp) as weekday
                            FROM
                            dftab
                            ORDER BY 1
                           """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite")\
                 .parquet(output_data + "time_table.parquet/")

    # read in song data to use for songplays table
    bucketname = input_data[6: ]
    bucketname = bucketname[0:bucketname.find("/")]
    
    lcobj = list(s3.list_objects_v2(Bucket=bucketname, 
                                Prefix="song_data/").values())
    
    song_data = []
    for k in lcobj[2]:
        if k["Key"].find(".json") > -1:
            song_data.append("s3a://" + bucketname + "/" + k["Key"])
    
    # read song data file
    song_df = spark.read.json(song_data)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://christophndde4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
