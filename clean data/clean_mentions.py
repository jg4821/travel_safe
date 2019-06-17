from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql import SparkSession
import configparser
import os


def main():
    # get aws credentials for accessing S3
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('default', 'aws_access_key_id')
    access_key = config.get('default', 'aws_secret_access_key')
    # initialize spark session
    spark = SparkSession.builder.appName('clean-mentions-data').config('spark.executor.memory', '6gb').getOrCreate()
    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3n.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')
    hadoop_conf.set('fs.s3n.awsAccessKeyId', access_id)
    hadoop_conf.set('fs.s3n.awsSecretAccessKey', access_key)
    sqlContext = SQLContext(sc)

    mentionsSchema =  StructType([
            StructField('GLOBALEVENTID',StringType(),True),
            StructField('EventTimeDate',StringType(),True),
            StructField('MentionTimeDate',StringType(),True),
            StructField('MentionType',StringType(),True),
            StructField('MentionSourceName',StringType(),True),
            StructField('MentionIdentifier',StringType(),True),
            StructField('SentenceID',StringType(),True),
            StructField('Actor1CharOffset',StringType(),True),
            StructField('Actor2CharOffset',StringType(),True),
            StructField('ActionCharOffset',StringType(),True),
            StructField('InRawText',StringType(),True),
            StructField('Confidence',StringType(),True),
            StructField('MentionDocLen',StringType(),True),
            StructField('MentionDocTone',StringType(),True),
            StructField('MentionDocTranslationInfo',StringType(),True),
            StructField('Extras',StringType(),True)])

    df = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter='\t') \
        .load('s3a://gdelt-open-data/v2/mentions/*', schema = mentionsSchema)
    # print('number of raw eventmentions: '+str(df.count()))

    filtered_df = df.filter('Confidence > 50')
    # print('number of filtered eventmentions: '+str(filtered_df.count()))

    base = 's3a://joy-travel-safe-bucket/cleaned-mentions-parquet/'
    filtered_df.write.parquet(base)

if __name__ == '__main__':
    main()
