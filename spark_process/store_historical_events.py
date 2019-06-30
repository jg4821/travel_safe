from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
import postgres

class ImportEventsToDB(object):
    def __init__(self, file_path):
        self.spark = SparkSession \
                    .builder \
                    .appName('store-events-to-db') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()

        self.file_path = file_path

    def read_parquet_from_s3(self):
        df = self.spark.read.parquet(self.file_path)
        return df

    def transform_df(self, df):
        df = df.withColumn('GLOBALEVENTID', df.GLOBALEVENTID.cast('INT'))
        df = df.withColumn('SQLDATE', F.to_timestamp(df.SQLDATE, format='yyyyMMdd'))
        df = df.withColumn('MonthYear', df.MonthYear.cast('INT'))
        df = df.withColumn('Month', F.month(df.SQLDATE))
        df = df.withColumn('Year', df.Year.cast('INT'))
        df = df.withColumn('Actor1Code', df.Actor1Code.cast('STRING'))
        df = df.withColumn('Actor1Name', df.Actor1Name.cast('STRING'))
        df = df.withColumn('Actor1CountryCode', df.Actor1CountryCode.cast('STRING'))
        df = df.withColumn('Actor1KnownGroupCode', df.Actor1KnownGroupCode.cast('STRING'))
        df = df.withColumn('Actor1EthnicCode', df.Actor1EthnicCode.cast('STRING'))
        df = df.withColumn('Actor1Religion1Code', df.Actor1Religion1Code.cast('STRING'))
        df = df.withColumn('Actor1Religion2Code', df.Actor1Religion2Code.cast('STRING'))
        df = df.withColumn('Actor1Type1Code', df.Actor1Type1Code.cast('STRING'))
        df = df.withColumn('Actor1Type2Code', df.Actor1Type2Code.cast('STRING'))
        df = df.withColumn('Actor1Type3Code', df.Actor1Type3Code.cast('STRING'))

        df = df.withColumn('Actor2Code', df.Actor2Code.cast('STRING'))
        df = df.withColumn('Actor2Name', df.Actor2Name.cast('STRING'))
        df = df.withColumn('Actor2CountryCode', df.Actor2CountryCode.cast('STRING'))
        df = df.withColumn('Actor2KnownGroupCode', df.Actor2KnownGroupCode.cast('STRING'))
        df = df.withColumn('Actor2EthnicCode', df.Actor2EthnicCode.cast('STRING'))
        df = df.withColumn('Actor2Religion1Code', df.Actor2Religion1Code.cast('STRING'))
        df = df.withColumn('Actor2Religion2Code', df.Actor2Religion2Code.cast('STRING'))
        df = df.withColumn('Actor2Type1Code', df.Actor2Type1Code.cast('STRING'))
        df = df.withColumn('Actor2Type2Code', df.Actor2Type2Code.cast('STRING'))
        df = df.withColumn('Actor2Type3Code', df.Actor2Type3Code.cast('STRING'))

        df = df.withColumn('IsRootEvent', df.IsRootEvent.cast('INT'))
        df = df.withColumn('EventCode', df.EventCode.cast('STRING'))
        df = df.withColumn('EventBaseCode', df.EventBaseCode.cast('STRING'))
        df = df.withColumn('EventRootCode', df.EventRootCode.cast('STRING'))
        df = df.withColumn('QuadClass', df.QuadClass.cast('INT'))
        df = df.withColumn('GoldsteinScale', df.GoldsteinScale.cast('FLOAT'))
        df = df.withColumn('NumMentions', df.NumMentions.cast('INT'))
        df = df.withColumn('NumSources', df.NumSources.cast('INT'))
        df = df.withColumn('NumArticles', df.NumArticles.cast('INT'))
        df = df.withColumn('AvgTone', df.AvgTone.cast('FLOAT'))

        df = df.withColumn('Actor1Geo_Type', df.Actor1Geo_Type.cast('INT'))
        df = df.withColumn('Actor1Geo_FullName', df.Actor1Geo_FullName.cast('STRING'))
        df = df.withColumn('Actor1Geo_CountryCode', df.Actor1Geo_CountryCode.cast('STRING'))
        df = df.withColumn('Actor1Geo_ADM1Code', df.Actor1Geo_ADM1Code.cast('STRING'))
        df = df.withColumn('Actor1Geo_ADM2Code', df.Actor1Geo_ADM2Code.cast('STRING'))
        df = df.withColumn('Actor1Geo_Lat', df.Actor1Geo_Lat.cast('FLOAT'))
        df = df.withColumn('Actor1Geo_Long', df.Actor1Geo_Long.cast('FLOAT'))
        df = df.withColumn('Actor1Geo_FeatureID', df.Actor1Geo_FeatureID.cast('STRING'))

        df = df.withColumn('Actor2Geo_Type', df.Actor2Geo_Type.cast('INT'))
        df = df.withColumn('Actor2Geo_FullName', df.Actor2Geo_FullName.cast('STRING'))
        df = df.withColumn('Actor2Geo_CountryCode', df.Actor2Geo_CountryCode.cast('STRING'))
        df = df.withColumn('Actor2Geo_ADM1Code', df.Actor2Geo_ADM1Code.cast('STRING'))
        df = df.withColumn('Actor2Geo_ADM2Code', df.Actor2Geo_ADM2Code.cast('STRING'))
        df = df.withColumn('Actor2Geo_Lat', df.Actor2Geo_Lat.cast('FLOAT'))
        df = df.withColumn('Actor2Geo_Long', df.Actor2Geo_Long.cast('FLOAT'))
        df = df.withColumn('Actor2Geo_FeatureID', df.Actor2Geo_FeatureID.cast('STRING'))

        df = df.withColumn('ActionGeo_Type', df.ActionGeo_Type.cast('INT'))
        df = df.withColumn('ActionGeo_FullName', df.ActionGeo_FullName.cast('STRING'))
        df = df.withColumn('ActionGeo_CountryCode', df.ActionGeo_CountryCode.cast('STRING'))
        df = df.withColumn('ActionGeo_ADM1Code', df.ActionGeo_ADM1Code.cast('STRING'))
        df = df.withColumn('ActionGeo_ADM2Code', df.ActionGeo_ADM2Code.cast('STRING'))
        df = df.withColumn('ActionGeo_Lat', df.ActionGeo_Lat.cast('FLOAT'))
        df = df.withColumn('ActionGeo_Long', df.ActionGeo_Long.cast('FLOAT'))
        df = df.withColumn('ActionGeo_FeatureID', df.ActionGeo_FeatureID.cast('STRING'))

        df = df.withColumn('SOURCEURL', df.SOURCEURL.cast('STRING'))

        df = df.drop('FractionDate','DATEADDED')

        return df

    def write_events_to_db(self, df):
        table = 'events'
        mode = 'append'
        
        connector = postgres.PostgresConnector()
        connector.write_to_db(df, table, mode)

    def run(self):
        parquet_df = self.read_parquet_from_s3()
        parquet_df.printSchema()
        
        out_df = self.transform_df(parquet_df)
        out_df.printSchema()
        
        self.write_events_to_db(out_df)
        
        print('Events write to db finished.')


def main():
    file_path = 's3a://joy-travel-safe-bucket/cleaned-events-parquet/'

    process = ImportEventsToDB(file_path)
    process.run()


if __name__ == '__main__':
    main()


