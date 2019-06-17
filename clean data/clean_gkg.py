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
    spark = SparkSession.builder.appName('clean-gkg-data-filtered').config('spark.executor.memory', '6gb').getOrCreate()
    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3n.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')
    hadoop_conf.set('fs.s3n.awsAccessKeyId', access_id)
    hadoop_conf.set('fs.s3n.awsSecretAccessKey', access_key)
    sqlContext = SQLContext(sc)

    gkgSchema =  StructType([
            StructField('GKGRECORDID',StringType(),True),
            StructField('DATE',StringType(),True),
            StructField('SourceCollectionIdentifier',StringType(),True),
            StructField('SourceCommonName',StringType(),True),
            StructField('DocumentIdentifier',StringType(),True),
            StructField('Counts',StringType(),True),
            StructField('V2Counts',StringType(),True),
            StructField('Themes',StringType(),True),
            StructField('V2Themes',StringType(),True),
            StructField('Locations',StringType(),True),
            StructField('V2Locations',StringType(),True),
            StructField('Persons',StringType(),True),
            StructField('V2Persons',StringType(),True),
            StructField('Organizations',StringType(),True),
            StructField('V2Organizations',StringType(),True),
            StructField('V2Tone',StringType(),True),
            StructField('Dates',StringType(),True),
            StructField('GCAM',StringType(),True),
            StructField('SharingImage',StringType(),True),
            StructField('RelatedImageEmbeds',StringType(),True),
            StructField('SocialImageEmbeds',StringType(),True),
            StructField('SocialVideoEmbeds',StringType(),True),
            StructField('Quotations',StringType(),True),
            StructField('AllNames',StringType(),True),
            StructField('Amounts',StringType(),True),
            StructField('TranslationInfo',StringType(),True),
            StructField('Extras',StringType(),True)])

    df = sqlContext.read \
        .format('com.databricks.spark.csv') \
        .options(header='false') \
        .options(delimiter='\t') \
        .load('s3a://gdelt-open-data/v2/gkg/*', schema = gkgSchema)
    # print('number of raw gkg: '+str(df.count()))

    filtered_df = df.drop('Themes','V2Themes','GCAM','SharingImage', \
        'RelatedImageEmbeds','SocialImageEmbeds','SocialVideoEmbeds', \
        'Quotations','AllNames','Amounts','TranslationInfo','Extras')
    # print('number of filtered gkg: '+str(filtered_df.count()))

    base = 's3a://joy-travel-safe-bucket/cleaned-gkgdata-parquet/'
    filtered_df.write.parquet(base)

if __name__ == '__main__':
    main()
