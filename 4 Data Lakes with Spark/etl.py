import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    ''' Create a Spark Session and return the spark session object'''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/A/A/B/*.json'
    print('read song_data json files')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'duration', 'year']).dropDuplicates()
    print('exrtacted data for songs_table')
    
    # write songs table to parquet files partitioned by year and artist
    #songs_table = songs_table.write().partitionBy(['year', 'artist_id']).format("parquet").save(os.path.join(output_data, "songs.parquet"))
    print('saved songs_table')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_lattitude', 'artist_longitude']).dropDuplicates()
    
    # write artists table to parquet files
    #artists_table = artists_table.write().format("parquet").save(os.path.join(output_data, "artist.parquet"))


# def process_log_data(spark, input_data, output_data):
#     # get filepath to log data file
#     log_data = input_data+'log_data/2018/11/*.json'

#     # read log data file
#     df = spark.read.json(log_data)
    
#     # filter by actions for song plays
#     df = 

#     # extract columns for users table    
#     artists_table = 
    
#     # write users table to parquet files
#     artists_table

#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
#     # extract columns to create time table
#     time_table = 
    
#     # write time table to parquet files partitioned by year and month
#     time_table

#     # read in song data to use for songplays table
#     song_df = 

#     # extract columns from joined song and log datasets to create songplays table 
#     songplays_table = 

#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #output_data = "s3a://praveen-practice-bucket/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
