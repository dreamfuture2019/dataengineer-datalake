import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create and Return a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This procedure processes song files from S3 and load into DataFrame to extract and then store into DataLake
    Parameters:
    * spark: the Spark Session variable
    * input_data: the s3 data source path variable
    * output_data: the s3 path variable to store dimensional tables
    """
    # get filepath to song data file
    song_data =  os.path.join(input_data, 'song_data', '*', '*', '*', '*.json')
    
    # read song data file
    df = spark.read.load(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']) \
                    .dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']) \
                      .dropDuplicates(['artist_id']) \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude')
                      
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    This procedure processes log files from S3 and load into DataFrame to extract and then store into DataLake
    Parameters:
    * spark: the Spark Session variable
    * input_data: the s3 data source path variable
    * output_data: the s3 path variable to store dimensional tables
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*', '*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']) \
                      .dropDuplicates(['userId']) \
                      .withColumnRenamed('userId', 'user_id') \
                      .withColumnRenamed('firstName', 'first_name') \
                      .withColumnRenamed('lastName', 'last_name')
                      
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(date_convert, TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))  
    
    # extract columns to create time table
    time_table =  df.select('start_time') \
                           .dropDuplicates(['start_time']) \
                           .withColumn('hour', hour('start_time')) \
                           .withColumn('day', dayofmonth('start_time')) \
                           .withColumn('week', weekofyear('start_time')) \
                           .withColumn('month', month('start_time')) \
                           .withColumn('year', year('start_time')) \
                           .withColumn('weekday', dayofweek('start_time'))
                           
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data, 'times'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*'))
    song_df.createOrReplaceTempView('songs')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table.createOrReplaceTempView('events')
    
    songplays_table = spark.sql("""
            SELECT  event.songplay_id,
                    event.start_time, 
                    event.userId as user_id, 
                    event.level, 
                    song.song_id,
                    song.artist_id, 
                    event.sessionId as session_id,
                    event.location, 
                    event.userAgent as user_agent
                    year(event.start_time) as year,
                    month(event.start_time) as month
            FROM events event, songs song
            WHERE event.page = 'NextSong' 
            AND event.song = song.title 
            AND event.artist = song.artist_name 
            AND event.length = song.duration
    """)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    """
    - Create a Spark Session
    - Load and Transform songs and logs file from DataSource into DataLake
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-udacity-haipd4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
