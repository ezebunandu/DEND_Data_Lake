import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (dayofmonth, dayofweek, hour,
                                   monotonically_increasing_id, month, udf,
                                   weekofyear, year)

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


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
    songs_table.write.mode("overwrite") \
                     .partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude',
                              'artist_longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(
                             os.path.join(output_data, 'artists')
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
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data, mode='PERMISSIVE',
                         columnNameOfCorruptRecord='corrupt_record')

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    user_table = df.select('userId', 'firstName', 'lastName',
                           'gender', 'level').dropDuplicates()

    # write users table to parquet files
    user_table.write.mode('overwrite').parquet(output_data + '/users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000))
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn('start_time', get_datetime('ts'))

    # extract columns to create time table
    time_table = df.withColumn("hour", hour('start_time')) \
                   .withColumn("day", dayofmonth('start_time')) \
                   .withColumn("week", weekofyear('start_time')) \
                   .withColumn("month", month('start_time')) \
                   .withColumn("year", year('start_time')) \
                   .withColumn("weekday", dayofweek('start_time')) \
                   .select("ts", "start_time", "hour",
                           "day", "week", "month",
                           "year", "weekday").dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite') \
                    .partitionBy('year', 'month') \
                    .parquet(output_data + '/time')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets
    # to create songplays table
    song_df.createOrReplaceTempView("songs_table")
    df.createOrReplaceTempView("logs_table")

    songplays_table = spark.sql('''
        SELECT DISTINCT
            l.start_time AS start_time,
            l.userId AS user_id,
            l.level AS level,
            s.song_id As song_id,
            s.artist_id AS artist_id,
            l.sessionId AS session_id,
            l.location AS location,
            l.userAgent AS user_agent,
            EXTRACT(month FROM l.start_time) AS month,
            EXTRACT(year FROM l.start_time) AS year
        FROM logs_table l
        JOIN songs_table s ON s.title=l.song AND s.artist_name=l.artist
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn('songplay_id',
                                                 monotonically_increasing_id())
    songplays_table.write.partitionBy('year', 'month') \
                         .parquet(os.path.join(output_data, 'songplays'),
                                  'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://hezeb-udacity-data-lake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
