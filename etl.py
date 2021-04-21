import configparser
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit, split, initcap, trim, concat, lower, translate
from pyspark.sql.functions import year
from pyspark.sql.types import IntegerType, FloatType
import sys
from itertools import chain
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)


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

def parse_immigration_dictionary(input_bucket):
    """
    - Let's read in the .SAS dictionary
    - then return a python dictionary from the file in order to look up state codes and cities
    """

    # we're going to use this to create the new data model and swap out numbers from the sas files 
    # replacing them with actual values. 
    input_bucket = input_bucket.replace('s3a://', '')
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name=input_bucket, key='I94_SAS_Labels_Descriptions.SAS')
    response = obj.get()

    f_content = response['Body'].read().decode('utf-8') 
    f_content = f_content.replace("'", '').split('\n')
    f_content = [x for x in f_content if '=' in x]

    immigration_dict = {x.split('=')[0].strip():x.split('=')[1].strip() for x in f_content}

    return immigration_dict

def process_immigration_data(spark, input_bucket, output_bucket):
    """
    - Here we are going load the s3 immigration files into a spark dataframe
    - We transform it to a new dataframe with specific columns for immigration and fact tables
    - Then we write it to parquet files in it's own directories
    """
    # create function to map immigration codes trhough our immigration dictionary
    # and return states, cities and countries
    immigration_dict = parse_immigration_dictionary(input_bucket)
    mapping_expr = create_map([lit(x) for x in chain(*immigration_dict.items())])
    
    # read in immigration data for april
    immigration_data_path = f'{input_bucket}/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data_path)

    immigration_df = immigration_df.withColumn("i94cit_int", immigration_df["i94cit"].cast(IntegerType()))

    immigration_table = immigration_df['cicid', 'i94port', 'i94cit_int']

    immigration_table = immigration_table.withColumn('arrival_area',  mapping_expr.getItem(col("i94port")))
    immigration_table = immigration_table.withColumn('origin_country',  mapping_expr.getItem(col("i94cit_int")))
    immigration_table = immigration_table.withColumn('arrival_city_stage', split(col('arrival_area'), ',').getItem(0))
    immigration_table = immigration_table.withColumn('state_code', trim(split(col('arrival_area'),',').getItem(1)))
    immigration_table = immigration_table.withColumn('arrival_city', initcap(col('arrival_city_stage')))

    immigration_table = immigration_table['cicid', 'origin_country', 'arrival_city', 'state_code'].dropDuplicates()
    
    # write immigrant_table table to parquet files
    immigration_table.write.mode('overwrite').parquet(output_bucket+'/immigration_table/immigration_table.parquet')

    dg_table = spark.table('dg_table')
    temp_table = spark.table('temp_table')
   
    immigrant_fact_table = immigration_table.join(dg_table, (immigration_table.arrival_city == dg_table.city)
                                                            & (immigration_table.state_code == dg_table.state_code))\
                                            .join(temp_table, (immigration_table.arrival_city == temp_table.City)
                                                            & (temp_table.Country == 'United States')).dropDuplicates()   

    immigrant_fact_table = immigrant_fact_table['cicid', 'temp_id', 'dg_id'].drop_duplicates()
       
    # write immigrant_fact_table table to parquet files
    immigrant_fact_table.write.mode('overwrite').parquet(output_bucket+'/immigrant_fact_table/immigrant_fact_table.parquet')


def process_temperature_data(spark, input_bucket, output_bucket):
    """
    - Here we are going load the s3 temperature files into a spark dataframe
    - We transform it to a new dataframe with specific columns for the temperature table
    - Then we write it to parquet files in it's own directories
    """

    # get filepath to temperature data file
    temperature_data_path = f'{input_bucket}/GlobalLandTemperaturesByCity.csv'

    # read in the temperature data
    temp_df = spark.read.options(inferSchema='True', delimiter=',', header='True').csv(temperature_data_path)

    # from here we can start to extract the relevant fields and create a new table
    temp_df = temp_df.where(year('dt') > 2007)
    temp_table = temp_df['dt', "City", "AverageTemperature", "Country"].groupBy('City', 'Country')\
                    .avg('AverageTemperature').dropDuplicates()
    temp_table = temp_table.withColumn("AverageTemperature", temp_table["avg(AverageTemperature)"].cast(FloatType()))
    temp_table = temp_table.withColumn("temp_id", translate(lower(concat(col('City'), lit('_'), col('Country'))), " ", "_"))
    temp_table = temp_table['temp_id', 'City', 'Country', 'AverageTemperature']
 
    # write temperature table to parquet files
    temp_table.write.mode('overwrite').parquet(output_bucket+'/temperature/temperature_table.parquet')

    # create a temp view in Spark to easily access data later for our fact table
    temp_table.createOrReplaceTempView('temp_table')


def process_demographic_data(spark, input_bucket, output_bucket):
    """
    - Here we are going load the s3 demographic files into a spark dataframe
    - We transform it to a new dataframe with specific columns for the demographic table
    - Then we write it to parquet files in it's own directories
    """

    # get filepath to demographic data file
    dg_data_path = f'{input_bucket}/us-cities-demographics.csv'

    # read in the demographic data
    dg_df = spark.read.options(inferSchema='True', delimiter=';', header='True').csv(dg_data_path)
    dg_df = dg_df.withColumnRenamed("Total Population", "total_population")
    dg_df = dg_df.withColumnRenamed("State Code", "state_code")

    # extract the relevant fields and create a new table
    dg_table = dg_df["city", "state", 'state_code', "total_population"].dropDuplicates()
    dg_table = dg_table.withColumn("dg_id", lower(concat(col('city'), lit('_'), col('state_code'))))
    dg_table = dg_table['dg_id', 'city', 'state_code', 'total_population']

    # write demographic table to parquet files
    dg_table.write.mode('overwrite').parquet(output_bucket+'/demographic/demographic_table.parquet')

    # create a temp view in Spark to easily access data later for our fact table
    dg_table.createOrReplaceTempView('dg_table')

def main():
    """
    - Here we initialize the spark session and set aws paths for our data
    """
    spark = create_spark_session()

    process_demographic_data(spark, input_bucket, output_bucket)
    process_temperature_data(spark, input_bucket, output_bucket)
    process_immigration_data(spark, input_bucket, output_bucket)

    spark.stop()


if __name__ == "__main__":
    main()
