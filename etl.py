import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window


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
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(f"{output_data}songs")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name AS name", "artist_location AS location", "artist_latitude AS latitude", "artist_longitude AS longitude") \
        .dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df["page"] == "NextSong")

    # extract columns for users table
    # applying a window function and retrieve the most recent entry of each userId
    users_window = Window.partitionBy("userId").orderBy(col("ts").desc())
    users_table = df.withColumn("row_number", row_number().over(users_window)).where(col("row_number") == 1) \
        .selectExpr("userId AS user_id", "firstName AS first_name", "lastName AS last_name", "gender", "level")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}users")

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = ""
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = ""
    
    # extract columns to create time table
    time_table = ""
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = ""

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = ""

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://dend-egarat/project_4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
