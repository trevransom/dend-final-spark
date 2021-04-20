import unittest
from etl import input_bucket, output_bucket, os, configparser
from pyspark.sql import SparkSession

# export PYSPARK_PYTHON=python3

config = configparser.ConfigParser()
config.read('dl.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

os.environ['AWS_ACCESS_KEY_ID'] = KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET
os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"

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


class TestETL(unittest.TestCase):
    spark = create_spark_session()

    def test_demographic_table_successful(self):
        print(f"{output_bucket}/demographic/demographic_table.parquet")
        test_df = self.spark.read.parquet(f"{output_bucket}/demographic/demographic_table.parquet")
        print(test_df.count())

        self.assertTrue(test_df.count() > 0)
        test_df.printSchema()

    def test_fact_table_successful(self):
        fact_df = self.spark.read.parquet(f"{output_bucket}/immigrant_fact_table/immigrant_fact_table.parquet")
        self.assertTrue(fact_df.count() < 0)
        print(fact_df.count())


if __name__ == '__main__':
    unittest.main()