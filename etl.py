import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config['USER_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']= config['USER_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']

#Create method to instance spark_session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    #Road file song_data
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    #Read song_data file
    df = spark.read.json(song_data).dropDuplicates() 
    
    #Extract columns to create songs table song
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    #Route file song en s3
    songFile = output_data+"songs"
    
    #Write songs table to parquet into s3 files partitioned by year and artist
    songs_table.write.mode("overwrite").format("parquet").partitionBy("year","artist_id").save(songFile)

    # extract columns to create artists table
    artists_table = df.select("artist_id",col("artist_name").alias('name'),col("artist_location").alias('location'),\
                              col("artist_latitude").alias('latitude'),col("artist_longitude").alias('longitude'))

    #Route where we will load the artist table
    artistFile = output_data+"artist"
    
    # write artists table to parquet into s3 files
    artists_table.write.mode("overwrite").format("parquet").save(artistFile)

def process_log_data(spark, input_data, output_data):
    #Get filepath to log_data file
    log_data = input_data + 'log_data/*/*/*.json'

    #Read log_data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'").dropDuplicates()

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").where("userId is not null").dropDuplicates()
    
    #Route file users en s3
    usersFile = output_data+"users"
    
    # write users table to parquet files
    users_table.write.mode("overwrite").format("parquet").save(usersFile)
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: str(int(int(t)/1000)))
    df = df.withColumn("timestamp", get_timestamp(col('ts')))
                     
    #Create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn("datetime", get_datetime(col("ts"))) 
                  
    #Extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year'),
                           date_format('datetime', 'F').alias('weekday')
                           )
    
    #Route file time en s3
    timeFile = output_data+"time"
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").format("parquet").partitionBy("year", "month").save(timeFile)

    #Road file song_data
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_data = input_data +'song_data/A/B/C/TRABCEI128F424C983.json'
    
    #Read song data file
    df = spark.read.json(song_data).dropDuplicates()
    
    #Extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.artist == song_df.artist_name & song_df.title == df.song)\
    .witColumn("songplay_id", monotonically_increasing_id()).\
    select(col('datetime').alias('start_time'), col('userId').alias('user_id'), 'level', "song_id","artist_id",\
           col("sessionId").alias("session_id"), col("artist_location").alias("location"),'userAgent',\
           year(col("start_time")).alias("year"),month(col("start_time")).alias("month"))
    
    #Road file songplays
    songplaysFile = output_data+"songplays"
    
    #Write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").format("parquet").partitionBy('year','month').save(songplaysFile)
    

def main():
    spark = create_spark_session()
    #Route input in s3
    input_data = "s3a://udacity-dend/"
    #Route output in s3
    output_data = "s3a://cargafiles/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
