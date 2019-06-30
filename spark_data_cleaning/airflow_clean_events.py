from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import configparser
import os
import sys
import schema


def main():
	# get aws credentials for accessing S3
	config = configparser.ConfigParser()
	config.read(os.path.expanduser('~/.aws/credentials'))
	access_id = config.get('default', 'aws_access_key_id')
	access_key = config.get('default', 'aws_secret_access_key')
	# initialize spark session
	spark = SparkSession.builder.appName('clean-event-data').config('spark.executor.memory', '6gb').getOrCreate()
	sc=spark.sparkContext
	hadoop_conf=sc._jsc.hadoopConfiguration()
	hadoop_conf.set('fs.s3n.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')
	hadoop_conf.set('fs.s3n.awsAccessKeyId', access_id)
	hadoop_conf.set('fs.s3n.awsSecretAccessKey', access_key)
	sqlContext = SQLContext(sc)

	eventSchema = schema.GDELTDataSchema().getEventSchema()

	date = str(sys.argv[1])
	df = sqlContext.read \
		.format('com.databricks.spark.csv') \
		.options(header='false') \
		.options(delimiter='\t') \
		.load('s3a://gdelt-open-data/v2/events/{}*'.format(date), schema = eventSchema)
	# print('number of raw events: '+str(df.count()))

	filtered_df = df.filter('ActionGeo_Type=3 or ActionGeo_Type=4')
	# print('number of filtered events: '+str(filtered_df.count()))

	base = 's3a://joy-travel-safe-bucket/airflow-events-parquet-{}/'.format(date)
	filtered_df.write.parquet(base)

if __name__ == '__main__':
	main()
