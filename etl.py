import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, when
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, from_unixtime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    # The package `org.apache.hadoop:hadoop-aws:2.7.0` allows you to connect aws S3.
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, partial=False):
    '''
    load and transform the song data, saving results back to S3
    '''
    # get filepath to song data file
    # we use a switch for testing on smaller song files
    if partial:
        song_data = input_data + "/song-data/A/A/A/*.json"
    else:
        song_data = input_data + "/song-data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    """
    process songs table
    """

    # extract columns to create songs table
    song_cols = ["song_id", "title", "artist_id", "year", "duration", "artist_name"]
    songs_table = df.select(song_cols).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_name").mode('overwrite').parquet(output_data + "/songs")

    """
    process artists table
    """
    # extract columns to create artists table
    artists_cols = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_cols).distinct()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "/artists")

def process_log_data(spark, input_data, output_data, partial=False):
    '''
    load and transform the log data, saving results back to S3
    '''
    # get filepath to log data file
    log_data = input_data + "/log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")

    # add in some derived columns that will be needed
    df = df.withColumn("start_time", from_unixtime(col("ts")/1000)) \
        .withColumn('year', year('start_time')) \
        .withColumn('month', month('start_time'))
    
    """
    process users table
    """

    # extract columns for users table    
    users_cols = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_cols).distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "/users")

    """
    process time table
    """
    
#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 

    # extract columns to create time table
    time_table = df \
        .select(from_unixtime(col("ts")/1000).alias("start_time")) \
        .withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time')) \
        .distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "/time")

    """
    process songplays table
    """
    # read in song data to use for songplays table
    if partial:
        song_data = input_data + "/song-data/A/A/A/*.json"
    else:
        song_data = input_data + "/song-data/*/*/*/*.json"
    songs_df = spark.read.json(song_data)
    songs_table = songs_df \
        .selectExpr(["song_id", "title", "artist_id", "year as release_year", "duration", "artist_name"]) \
        .filter(col("song_id").isNotNull()) \
        .distinct()
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df \
        .join( \
            songs_table, \
            (df.artist == songs_table.artist_name) & (df.song == songs_table.title) & (df.length == songs_table.duration), \
            how='left' \
            )
#     songplays_table.printSchema() 
    songplays_table = songplays_table\
        .selectExpr(["start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionId as session_id", "location", "userAgent as user_agent", "year", "month"])

#     # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "/songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-ashercornelius/analytics/"
    
    process_song_data(spark, input_data, output_data, partial=True)    
    process_log_data(spark, input_data, output_data, partial=True)


if __name__ == "__main__":
    main()
