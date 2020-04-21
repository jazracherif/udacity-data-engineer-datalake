from datetime import datetime, date
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType


def create_spark_session():
    """ Create a Spark Session
    """    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Process the song datafiles and create the songs and artists dimension tables
        Write the output tables in the parquet format to output_data

        args:
            * spark: a sparkSession object
            * input_data: The url that includes the logs
            * output_data: the S3 bucket for the output files
    """

    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()

    # extract columns to create songs table
    # columns: song_id, title, artist_id, year, duration
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
                    .distinct()
    songs_table.createOrReplaceTempView("songs")

    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+"songs", 
                              mode='overwrite',
                              partitionBy=["year", "artist_id"]
                            )

    # extract columns to create artists table
    # columns: artist_id, name, location, lattitude, longitude
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    artists_table = artists_table.withColumnRenamed("artist_name", "name") \
                                 .withColumnRenamed("artist_location", "location") \
                                 .withColumnRenamed("artist_latitude", "latitude") \
                                 .withColumnRenamed("artist_longitude", "longitude") \
                                 .distinct()             

    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists", 
                              mode='overwrite',
                            )


def process_log_data(spark, input_data, output_data):
    """ Process the log datafiles and create the times, users, and songplays tables.
        Write the output tables in the parquet format to output_data

        args:
            * spark: a sparkSession object
            * input_data: The url that includes the logs
            * output_data: the S3 bucket for the output files
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    # columns: user_id, first_name, last_name, gender, level
    users = df.select(["ts", "userId", "firstName", "lastName", "gender", "level"]) \
                    .withColumnRenamed("userId", "user_id") \
                    .withColumnRenamed("firstName", "first_name") \
                    .withColumnRenamed("lastName", "last_name")

    users.createOrReplaceTempView("users")
    users_table = spark.sql(
            """
                WITH numbered_levels AS (
                  SELECT ROW_NUMBER() over (PARTITION by user_id ORDER BY ts DESC) AS row_num,
                         user_id,
                         first_name, 
                         last_name, 
                         gender, 
                         level
                    FROM users
                )
                SELECT DISTINCT user_id, first_name, last_name, gender, level
                  FROM numbered_levels
                 WHERE row_num = 1
            """
        )
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users", 
                              mode='overwrite',
                            )


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda epoch: datetime.fromtimestamp(epoch / 1000.0), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    # # create datetime column from original timestamp column
    get_datetime = udf(lambda epoch: date.fromtimestamp(epoch / 1000.0), DateType())
    df = df.withColumn("date", get_datetime("ts"))
    
    # extract columns to create time table
    # columns = start_time, hour, day, week, month, year, weekday
    time_table = df.select("start_time", 
                           hour("date").alias("hour"), 
                           dayofmonth("date").alias("day"), 
                           weekofyear("date").alias("week"), 
                           month("date").alias("month"),
                           year("date").alias("year"),
                           dayofweek("date").alias("weekday")
                        ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "times", 
                              mode='overwrite',
                              partitionBy=["year", "month"]
                            )

    # read in song data to use for songplays table
    song_df = df.select("song", 
                         "length", 
                         "page", 
                         "start_time",
                         "userId", 
                         "level", 
                         "sessionId",
                         "location", 
                         "userAgent",
                        month("date").alias("month"),
                        year("date").alias("year"),
                        )
  
    song_df.createOrReplaceTempView("staging_events")

    # extract columns from joined song and log datasets to create songplays table 
    # columns: songplay_id, start_time, user_id, level, song_id, 
    # artist_id, session_id, location, user_agent
    songplays_table = spark.sql(
            """
            SELECT e.start_time, 
                   e.userId AS user_id, 
                   e.level AS level, 
                   s.song_id AS song_id, 
                   s.artist_id AS artist_id, 
                   e.sessionId AS session_id, 
                   e.location AS location, 
                   e.userAgent AS user_agent,
                   e.year,
                   e.month
            FROM staging_events e
            LEFT JOIN songs s ON e.song = s.title
                    AND e.length = s.duration
                    AND e.year = s.year
           WHERE s.song_id IS NOT NULL
             AND s.artist_id IS NOT NULL
            """
        )

    print("songplays_table", songplays_table.limit(5).collect())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays", 
                              mode='overwrite',
                              partitionBy=["year", "month"]
                            )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://jazra-udacity-spark-etl/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
