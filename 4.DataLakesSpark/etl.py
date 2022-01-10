import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["AWS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["AWS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data["songs"] + "/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    songs_table = songs_table.distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(output_data + "/songs")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")\
                    .withColumnRenamed("artist_name", "name")\
                    .withColumnRenamed("artist_location", "location")\
                    .withColumnRenamed("artist_latitude", "latitude")\
                    .withColumnRenamed("artist_longitude", "longitude")\
                    .distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "/artists")
    
    return (songs_table, artists_table)


def process_log_data(spark, input_data, output_data, songs_table, artists_table):
    # get filepath to log data file
    log_data = input_data["logs"] + "/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")\
                .withColumnRenamed("userId", "user_id")\
                .withColumnRenamed("firstName", "first_name")\
                .withColumnRenamed("lastName", "last_name")\
                .distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "/users")

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select("ts", "datetime")\
                .withColumn("hour", hour(df.datetime))\
                .withColumn("day", dayofmonth(df.datetime))\
                .withColumn("week", weekofyear(df.datetime))\
                .withColumn("month", month(df.datetime))\
                .withColumn("year", year(df.datetime))\
                .withColumn("weekday", dayofweek(df.datetime))\
                .withColumnRenamed("ts", "start_time")\
                .distinct()
    
    time_table = time_table.drop("datetime")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "/time")

    # read in song data to use for songplays table
    # Songs support
    songs_support = songs_table.join(artists_table, songs_table.artist_id == artists_table.artist_id).select("song_id", "title",artists_table.artist_id, "duration", "name")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(songs_support, (df.song == songs_support.title) & (df.artist == songs_support.name) & (df.length == songs_support.duration) )\
                    .join(time_table, df.ts == time_table.start_time)\
                    .withColumn("id", monotonically_increasing_id())\
                    .select("id","ts", "year", "month", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent")\
                    .withColumnRenamed("ts", "start_time")\
                    .withColumnRenamed("userId", "user_id")\
                    .withColumnRenamed("sessionId", "session_id")\
                    .withColumnRenamed("userAgent", "user_agent")

    # write songplays table to parquet files
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "/songplays")


def main():
    spark = create_spark_session()
    # S3
    input_data = {
        "logs": "s3a://udacity-dend/log_data",
        "songs": "s3a://udacity-dend/song_data/"
    }
    output_data = "s3a://my-udacity-spark-bucket/output"
    
    songs_table, artists_table = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, songs_table, artists_table)
    spark.stop()


if __name__ == "__main__":
    main()
