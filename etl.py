import configparser
from datetime import datetime
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, create_map, lit, asc, desc, split, initcap, trim, concat, lower, translate
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, FloatType
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

from itertools import chain
import glob

config = configparser.ConfigParser()
config.read('dl.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

os.environ['AWS_ACCESS_KEY_ID'] = KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET
os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"

input_bucket = "s3a://dend-final-source-1"
output_bucket = "s3a://dend-final-output-1"


def create_spark_session():
    """
    - Here we are going initialize the spark session and return it as an object
    """
    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark

# need to set up actual emr cluster in order to write to Parquet

def test_parquet(spark, output_bucket):
    print(output_bucket+'/demographic/demographic_table.parquet')
    test_df = spark.read.parquet(output_bucket+'/demographic/demographic_table.parquet')
    test_df.printSchema()

def parse_immigration_dictionary(input_bucket):
    """
    - Let's read in the .SAS dictionary
    - then return a python dictionary from the file
    """
    # df, meta = pyreadstat.read_sas7bdat(fname)
    # print(df)

    # we're going to use this to create the new data model and swap out numbers from the sas files 
    # and replace them with actual values. 
    input_bucket = input_bucket.replace('s3a://', '')
    print(input_bucket)
    s3 = boto3.resource('s3')
    # bucket = s3.Bucket(input_bucket)
    obj = s3.Object(bucket_name=input_bucket, key='I94_SAS_Labels_Descriptions.SAS')
    response = obj.get()

    f_content = response['Body'].read().decode('utf-8') 
    f_content = f_content.replace("'", '').split('\n')
    f_content = [x for x in f_content if '=' in x]


    immigration_dict = {x.split('=')[0].strip():x.split('=')[1].strip() for x in f_content}

    return immigration_dict



def process_immigration_data(spark, input_bucket, output_bucket):

    # parse_data_udf = udf(lambda x: parse_immigration_dictionary(x))
    immigration_dict = parse_immigration_dictionary(input_bucket)
    mapping_expr = create_map([lit(x) for x in chain(*immigration_dict.items())])
    # mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])

    # print(immigration_dict)
    # df.withColumn("value", mapping_expr[col("I94PORT")]).show()

    # need to use all sas files now?

    immigration_data_path = f'{input_bucket}/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    print(immigration_data_path)
    immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data_path)
    # immigration_df = immigration_df.loadimit(1000)
    immigration_df = immigration_df.withColumn("i94cit_int", immigration_df["i94cit"].cast(IntegerType()))

    print(immigration_df.printSchema())
    immigration_df.sort('cicid').show(100)
    # will i need to loop through each row to look the the city from the code?
    # is there a batch way to do that?
    immigration_table = immigration_df['cicid', 'i94port', 'i94cit_int']
    immigration_table.sort('cicid').show(100)
    # immigration_table = immigration_table.map(lambda x: x[0], x[1]*2.2)


    # immigration_table.na.replace(immigration_dict, 1).show()

    immigration_table = immigration_table.withColumn('arrival_area',  mapping_expr.getItem(col("i94port")))
    immigration_table = immigration_table.withColumn('origin_country',  mapping_expr.getItem(col("i94cit_int")))
    immigration_table.sort('cicid').show(300)
    # df_states.withColumn("col1", split(col("State_Name"), ",").getItem(0)).withColumn("col2", split(col("State_Name"), "-").getItem(1)).show()

    immigration_table = immigration_table.withColumn('arrival_city_stage', split(col('arrival_area'), ',').getItem(0))
    immigration_table = immigration_table.withColumn('state_code', trim(split(col('arrival_area'),',').getItem(1)))
    immigration_table = immigration_table.withColumn('arrival_city', initcap(col('arrival_city_stage')))

    immigration_table = immigration_table['cicid', 'origin_country', 'arrival_city', 'state_code'].dropDuplicates()
    immigration_table.show()
    # print(immigration_table.head(100))
    # could do this with python or sql
    # could join immigration table and usse the data dicitoanry to add another column

# - cicid: double (nullable = true)
#  |-- i94yr: double (nullable = true)
#  |-- i94mon: double (nullable = true)
#  |-- i94cit: double (nullable = true)
#  |-- i94res: double (nullable = true)
#  |-- i94port: string (nullable = true)
#  |-- arrdate: double (nullable = true)
#  |-- i94mode: double (nullable = true)
#  |-- i94addr: string (nullable = true)
#  |-- depdate: double (nullable = true)
#  |-- i94bir: double (nullable = true)
#  |-- i94visa: double (nullable = true)
#  |-- count: double (nullable = true)
#  |-- dtadfile: string (nullable = true)
#  |-- visapost: string (nullable = true)
#  |-- occup: string (nullable = true)
#  |-- entdepa: string (nullable = true)
#  |-- entdepd: string (nullable = true)
#  |-- entdepu: string (nullable = true)
#  |-- matflag: string (nullable = true)
#  |-- biryear: double (nullable = true)
#  |-- dtaddto: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- insnum: string (nullable = true)
#  |-- airline: string (nullable = true)
#  |-- admnum: double (nullable = true)
#  |-- fltno: string (nullable = true)
#  |-- visatype: string (nullable = true)

    immigration_table.write.mode('overwrite').parquet(output_bucket+'/immigration_table/immigration_table.parquet')

    dg_table = spark.table('dg_table')
    temp_table = spark.table('temp_table')
    dg_table.show()
    temp_table.show()
        # city and state

    immigrant_fact_table = immigration_table.join(dg_table, (immigration_table.arrival_city == dg_table.city) \
                                            & (immigration_table.state_code == dg_table.state_code))\
                                            .join(temp_table, (immigration_table.arrival_city == temp_table.City)
                                            & (temp_table.Country == 'United States')).dropDuplicates()   

    immigrant_fact_table = immigrant_fact_table['cicid', 'temp_id', 'dg_id'].drop_duplicates()
    immigrant_fact_table.show(200)  
       

    # make sure to filter on temp for just countries USA
# read song data file
    # song_df = spark.read.json(song_data_path, schema=song_df_schema)

    # # extract columns from joined song and log datasets to create songplays table
    # songplays_table = log_df.join(song_df, (log_df.artist == song_df.artist_name) & (log_df.song == song_df.title) & (log_df.length == song_df.duration))
    # songplays_table = songplays_table.select(col('timestamp').alias('start_time'), col('userId').alias('user_id'),
    #                                          'level', 'song_id', 'artist_id', col('sessionId').alias('session_id'),
    #                                          'artist_location', col('userAgent').alias('user_agent'),
    #                                          month('timestamp').alias('month'), year('timestamp').alias('year')).drop_duplicates()

    # # write songplays table to parquet files partitioned by year and month
    immigrant_fact_table.write.mode('overwrite').parquet(output_bucket+'/immigrant_fact_table/immigrant_fact_table.parquet')


    # 
# immigrant (general info on travel from immigrants to the USA)
# cicid (serial?)
# city from
# city to
# country from

    print('hey')
# 18-83510-I94-Data-2016
    # for x in #allfiles:
    #     # test it with 100 first
    #     immigration_df = x.head(100)


def process_temperature_data(spark, input_bucket, output_bucket):
    temperature_data_path = f'{input_bucket}/GlobalLandTemperaturesByCity.csv'
    print (temperature_data_path)

    # trying to infer the schema for now
    temp_df = spark.read.options(inferSchema='True', delimiter=',', header='True').csv(temperature_data_path)
    # temp_df = dg_df.withColumnRenamed("Total Population", "total_population")

    temp_df.printSchema()

    # from here we can start to extract the relevant fields and create a new table
    # temp_df.filter(temp_df.year('dt') > 2007).collect() 
    temp_df = temp_df.where(year('dt') > 2007)
    temp_table = temp_df['dt', "City", "AverageTemperature", "Country"].groupBy('City', 'Country')\
                    .avg('AverageTemperature').dropDuplicates()
    temp_table = temp_table.withColumn("AverageTemperature", temp_table["avg(AverageTemperature)"].cast(FloatType()))
    temp_table = temp_table.withColumn("temp_id", translate(lower(concat(col('City'), lit('_'), col('Country'))), " ", "_"))
    temp_table = temp_table['temp_id', 'City', 'Country', 'AverageTemperature']
    # temp_table = temp_table.filter() ['dt', "City", "AverageTemperature", "Country"].dropDuplicates()

    temp_table.show(100, False)
    temp_table.write.mode('overwrite').parquet(output_bucket+'/temperature/temperature_table.parquet')

# create a unit test to ensure that the parquet files have data in them

    # remember to average the temp for one play
    # (only import data where dt year is > 2008)
#     city
# avg_temperature

    # # write songs table to parquet files partitioned by year and artist
    # dg_table.write.mode('overwrite').parquet(output_bucket+'/demographic/demographic_table.parquet', mode='overwrite')

    # test_df=spark.read.parquet(f"{output_bucket}/demographic/demographic_table.parquet")
    # test_df.printSchema()
    # test_df.show(10)
    print(temp_table.count())
    temp_table.createOrReplaceTempView('temp_table')



def process_demographic_data(spark, input_bucket, output_bucket):

    # get filepath to demographic data file
    # dg_data_path = f'{input_bucket}/../us-cities-demographics.csv'
    dg_data_path = f'{input_bucket}/us-cities-demographics.csv'
    print (dg_data_path)

    # explicitly define the dg_df schema
    # dg_df_schema = StructType({
    #     StructField('city', StringType(), True),
    #     StructField('artist_latitude', DoubleType(), True),
    #     StructField('artist_location', DoubleType(), True),
    #     StructField('artist_longitude', StringType(), True),
    #     StructField('artist_name', StringType(), True),
    #     StructField('duration', DoubleType(), True),
    #     StructField('num_songs', IntegerType(), True),
    #     StructField('song_id', StringType(), True),
    #     StructField('title', StringType(), True),
    #     StructField('year', IntegerType(), True)
    # })

    # trying to infer the schema for now
    dg_df = spark.read.options(inferSchema='True', delimiter=';', header='True').csv(dg_data_path)
    dg_df = dg_df.withColumnRenamed("Total Population", "total_population")
    dg_df = dg_df.withColumnRenamed("State Code", "state_code")
    

    dg_df.printSchema()
    # City    State   Median Age  Male Population Female Population   Total Population    Number of Veterans  Foreign-born    Average Household Size  State Code  Race    Count

    # from here we can start to extract the relevant fields and create a new table

    dg_table = dg_df["city", "state", 'state_code', "total_population"].dropDuplicates()
    dg_table = dg_table.withColumn("dg_id", lower(concat(col('city'), lit('_'), col('state_code'))))
    dg_table = dg_table['dg_id', 'city', 'state_code', 'total_population']

    print(dg_table.head())
    # write songs table to parquet files partitioned by year and artist
    dg_table.write.mode('overwrite').parquet(output_bucket+'/demographic/demographic_table.parquet')
    dg_table.createOrReplaceTempView('dg_table')

    # test_df=spark.read.parquet(f"{output_bucket}/demographic/demographic_table.parquet")
    # test_df.printSchema()
    # # test
    # test_df.show(10)
    # print(test_df.count())

def process_song_data(spark, input_data, output_bucket):
    """
    - Here we are going load the s3 song files into a spark dataframe
    - We transform it to a new dataframe with specific columns for song and artist tables
    - Then we write it to parquet files in it's own directories
    """

    # get filepath to song data file
    # currently just a sample of the full data for demonstration purposes
    song_data_path = f'{input_data}/song_data/A/A/*/*.json'

    # explicitly define the song_df schema
    song_df_schema = StructType({
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', DoubleType(), True),
        StructField('artist_longitude', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('year', IntegerType(), True)
    })

    # read song data file
    songs_data = spark.read.json(song_data_path, schema=song_df_schema)
    songs_data.persist()

    # extract columns to create songs table
    songs_table = songs_data["song_id", "title", "artist_id", "year", "duration"].dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_bucket+'/songs/song_table.parquet', mode='overwrite')

    # extract columns to create artists table
    artists_table = songs_data["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"].dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_bucket+'/artists/artists_table.parquet')

    # testing to ensure the parquet files got written
    songs_read_df = spark.read.parquet(output_bucket+'/artists/artists_table.parquet/*.parquet')
    print(songs_read_df.head())

def process_log_data(spark, input_data, output_bucket):
    """
    - Here we are going load the s3 data log files into a spark dataframe
    - We transform it to a new dataframe with specific columns for user, time and songplays tables
    - Then we write it to parquet files in it's own directories
    """

    # get filepath to log data file
    log_data_path = f'{input_data}/log_data/*/*/*.json'

    # explicitly define the song_df schema for reading in
    log_df_schema = StructType({
        StructField('artist', StringType(), True),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', IntegerType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DoubleType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), True),
        StructField('registration', DoubleType(), True),
        StructField('sessionId', IntegerType(), True),
        StructField('song', StringType(), True),
        StructField('status', IntegerType(), True),
        StructField('ts', LongType(), True),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), True)
    })

    # read log data file
    log_df = spark.read.json(log_data_path, schema=log_df_schema)

    log_df = log_df.filter(log_df.userId != '')
    log_df = log_df.withColumn("userId", log_df["userId"].cast(IntegerType()))

    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == "NextSong")

    # extract columns for users table
    users_table = log_df['userId', 'firstName', 'lastName', 'gender', 'level']

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_bucket+'/users/users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))

    # extract columns to create time table
    time_table = log_df[col('timestamp').alias('start_time'), hour('timestamp').alias('hour'),
                        dayofmonth('timestamp').alias('day'), weekofyear('timestamp').alias('week'),
                        month('timestamp').alias('month'), year('timestamp').alias('year'),
                        date_format('timestamp', 'E').alias('weekday')].drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_bucket+'/time/time_table.parquet')

    # read in song data to use for songplays table
    song_data_path = f'{input_data}/song_data/*/*/*/*.json'

    # explicitly define the song_df schema
    song_df_schema = StructType({
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', DoubleType(), True),
        StructField('artist_longitude', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('year', IntegerType(), True)
    })

    # read song data file
    song_df = spark.read.json(song_data_path, schema=song_df_schema)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = log_df.join(song_df, (log_df.artist == song_df.artist_name) & (log_df.song == song_df.title) & (log_df.length == song_df.duration))
    songplays_table = songplays_table.select(col('timestamp').alias('start_time'), col('userId').alias('user_id'),
                                             'level', 'song_id', 'artist_id', col('sessionId').alias('session_id'),
                                             'artist_location', col('userAgent').alias('user_agent'),
                                             month('timestamp').alias('month'), year('timestamp').alias('year')).drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_bucket+'/songplays/songplays_table.parquet')

def create_testing_pyspark_session():
 return (SparkSession.builder
 .master('local[2]')
 .appName('my-local-testing-pyspark-context')
 .enableHiveSupport()
 .getOrCreate())

def main():
    """
    - Here we initialize the spark session and set aws paths for our data
    """
    spark = create_spark_session()

    # run data dict here
    # save result to python local
    # pass dict to necessary function

    # data_dict = parse_immigration_dictionary(spark, input_bucket)

    process_demographic_data(spark, input_bucket, output_bucket)
    process_temperature_data(spark, input_bucket, output_bucket)
    process_immigration_data(spark, input_bucket, output_bucket)
    # process_song_data(spark, input_bucket, output_bucket)
    # process_log_data(spark, input_data, output_bucket)

    spark.stop()

    # return spark


if __name__ == "__main__":
    main()
