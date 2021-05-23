import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """
    Creates a SparkSession object used for the ETL process.

        Returns:
            spark (object): A reference to the SparkSession provided by the EMR cluster.
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads and transforms 'song_data' from source S3 bucket. Transformed data will be stored on target S3 bucket with the prefix 'songs' and 'artists' in Parquet file format.

        Parameters:
            spark (object): A reference to the SparkSession provided by the EMR cluster.
            input_data (string): String representing the source S3 bucket in the format "s3://<BUCKET_NAME>"
            output_data (string): String representing the target S3 bucket where the transformed Parquet files should be persisted.
    """

    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(f"{output_data}songs")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name AS name", "artist_location AS location", "artist_latitude AS latitude", "artist_longitude AS longitude") \
        .dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}artists")


def process_log_data(spark, input_data, output_data):
    """
    Loads and transforms 'data_data' from source S3 bucket. Transformed data will be stored on target S3 bucket with the prefix 'users', 'time', and 'songplays' in Parquet file format.

        Parameters:
            spark (object): A reference to the SparkSession provided by the EMR cluster.
            input_data (string): String representing the source S3 bucket in the format "s3://<BUCKET_NAME>"
            output_data (string): String representing the target S3 bucket where the transformed Parquet files should be persisted.
    """

    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df["page"] == "NextSong")

    # extract columns for users table
    # applying a window function to drop duplicate userId and retrieve only the most recent userId
    users_window = Window.partitionBy("userId").orderBy(col("ts").desc())
    users_table = df.withColumn("row_number", row_number().over(users_window)).where(col("row_number") == 1) \
        .selectExpr("userId AS user_id", "firstName AS first_name", "lastName AS last_name", "gender", "level")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}users")

    # create timestamp (yyy-MM-dd HH:mm:ss:SSS) column from original timestamp (unix timestamp) column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
  
    # extract columns to create time table
    time_table = df.selectExpr("timestamp AS start_time") \
                .withColumn("hour", hour("start_time")) \
                .withColumn("day", dayofmonth("start_time")) \
                .withColumn("week", weekofyear("start_time")) \
                .withColumn("month", month("start_time")) \
                .withColumn("year", year("start_time")) \
                .withColumn("weekday", dayofweek("start_time")) \
                .dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_data}time")

    # read in song data to use for songplays table
    song_df = spark.read.json(f"{input_data}song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table
    songplays_id_window = Window.orderBy(col("timestamp"))
    songplays_table = df.join(song_df, (df["song"] == song_df["title"]) & (df["artist"] == song_df["artist_name"])).withColumn("songplay_id", row_number().over(songplays_id_window)) \
        .selectExpr("songplay_id", "timestamp AS start_time", "userId AS user_id", "level", "song_id", "artist_id", "sessionId AS session_id", "location", "userAgent AS user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table \
        .withColumn("year", year("start_time")).withColumn("month", month("start_time")) \
        .write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_data}songplays")


def main():
    """
    Entry function that will trigger the ETL process. The process consists of the following steps:
        - Extract JSON source files from S3 bucket 'udacity-dend' and load them into Spark dataframes
        - Perform transformations on source dataframes and load them into entities dataframes
        - Write entities dataframes on target S3 bucket as Parquet with a pre-defined S3 prefix representing the entity
    """

    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://dend-egarat/project_4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
