import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, dayofmonth, dayofweek, hour,
                                   monotonically_increasing_id, month, udf,
                                   weekofyear, year)
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    return SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()


def process_song_data(spark, input_data, output_data):
    """
    Read all song data files into Spark and transform
    into song and artist tables. Store the song and
    artist tables as parquet files.

    Parameters
    ----------
    spark:
        The spark session to use for processing
    input_data: string
        The path to the input files in S3
    output_data: string
        The path to store the generated parquet files.
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data, mode='PERMISSIVE',
                         columnNameOfCorruptRecord='corrupt_record')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'artist_id',
                            'title', 'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(
                           os.path.join(output_data, 'song_table'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude',
                              'artist_longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(
                             os.path.join(output_data, 'artists_table')
    )


def process_log_data(spark, input_data, output_data):
    """
    Process the event log file and extract data for time,
    users, and songplays tables from it.

    Parameters
    ----------
    spark:
        The spark session to use
    input_data: string
        The path to the input log files (in s3)
    output_data: string
        The path to store the generated parquet files
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data, mode='PERMISSIVE',
                         columnNameOfCorruptRecord='corrupt_record')

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    user_table = df.select('userId', 'firstName', 'lastName',
                           'gender', 'level').dropDuplicates()

    # write users table to parquet files
    user_table.write.mode('overwrite').parquet(output_data + '/user_table')

    # create timestamp column from original timestamp column
    def get_ts(ts):
        return datetime.fromtimestamp(ts / 1000)

    get_timestamp = udf(lambda x: get_ts(x), TimestampType())

    # create datetime column from original timestamp column
    df = df.withColumn('start_time', get_timestamp('ts'))

    # extract columns to create time table
    time_table = df.withColumn("hour", hour('start_time')) \
                   .withColumn("day", dayofmonth('start_time')) \
                   .withColumn("week", weekofyear('start_time')) \
                   .withColum("month", month('start_time')) \
                   .withColumn("year", year('start_time')) \
                   .withColumn("weekday", dayofweek('start_time')) \
                   .select("ts", "start_time", "hour",
                           "day", "week", "month",
                           "year", "weekday").dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite') \
                    .paritionBy('year', 'month') \
                    .parquet(output_data + '/time_table')

    # read in song data to use for songplays table
    song_df = spark.read \
                   .format('parquet')\
                   .option("basePath", os.path.join(output_data, "songs/"))\
                   .load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets
    # to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(monotonically_increasing_id()
                                .alias("songplay_id"),
                                col('start_time'),
                                col('userID').alias("user_id"),
                                "level", "song_id", "artist_id",
                                col("sessionId").alias("session_id"),
                                "location",
                                col("userAgent").alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.dropDuplicates() \
                   .write.parquet(os.path.join(output_data, "songplays/"),
                                  mode="overwrite",
                                  partitionBy=["year", "month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
