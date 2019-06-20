from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
import timescale

class ImportEventsToDB(object):
    def __init__(self, file_path):
        self.spark = SparkSession \
                    .builder \
                    .appName('store-events-to-db') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()

        # event_schema = [
        #     ('GLOBALEVENTID','INT'),
        #     ('SQLDATE','STRING'),
        #     ('MonthYear','INT'),
        #     ('Year','INT'),
        #     ('FractionDate','FLOAT'),
        #     ('Actor1Code','STRING'),
        #     ('Actor1Name','STRING'),
        #     ('Actor1CountryCode','STRING'),
        #     ('Actor1KnownGroupCode','STRING'),
        #     ('Actor1EthnicCode','STRING'),
        #     ('Actor1Religion1Code','STRING'),
        #     ('Actor1Religion2Code','STRING'),
        #     ('Actor1Type1Code','STRING'),
        #     ('Actor1Type2Code','STRING'),
        #     ('Actor1Type3Code','STRING'),
        #     ('Actor2Code','STRING'),
        #     ('Actor2Name','STRING'),
        #     ('Actor2CountryCode','STRING'),
        #     ('Actor2KnownGroupCode','STRING'),
        #     ('Actor2EthnicCode','STRING'),
        #     ('Actor2Religion1Code','STRING'),
        #     ('Actor2Religion2Code','STRING'),
        #     ('Actor2Type1Code','STRING'),
        #     ('Actor2Type2Code','STRING'),
        #     ('Actor2Type3Code','STRING'),
        #     ('IsRootEvent','INT'),
        #     ('EventCode','STRING'),
        #     ('EventBaseCode','STRING'),
        #     ('EventRootCode','STRING'),
        #     ('QuadClass','INT'),
        #     ('GoldsteinScale','FLOAT'),
        #     ('NumMentions','INT'),
        #     ('NumSources','INT'),
        #     ('NumArticles','INT'),
        #     ('AvgTone','FLOAT'),
        #     ('Actor1Geo_Type','INT'),
        #     ('Actor1Geo_FullName','STRING'),
        #     ('Actor1Geo_CountryCode','STRING'),
        #     ('Actor1Geo_ADM1Code','STRING'),
        #     ('Actor1Geo_ADM2Code','STRING'),
        #     ('Actor1Geo_Lat','FLOAT'),
        #     ('Actor1Geo_Long','FLOAT'),
        #     ('Actor1Geo_FeatureID','STRING'),
        #     ('Actor2Geo_Type','INT'),
        #     ('Actor2Geo_FullName','STRING'),
        #     ('Actor2Geo_CountryCode','STRING'),
        #     ('Actor2Geo_ADM1Code','STRING'),
        #     ('Actor2Geo_ADM2Code','STRING'),
        #     ('Actor2Geo_Lat','FLOAT'),
        #     ('Actor2Geo_Long','FLOAT'),
        #     ('Actor2Geo_FeatureID','STRING'),
        #     ('ActionGeo_Type','INT'),
        #     ('ActionGeo_FullName','STRING'),
        #     ('ActionGeo_CountryCode','STRING'),
        #     ('ActionGeo_ADM1Code','STRING'),
        #     ('ActionGeo_ADM2Code','STRING'),
        #     ('ActionGeo_Lat','FLOAT'),
        #     ('ActionGeo_Long','FLOAT'),
        #     ('ActionGeo_FeatureID','STRING'),
        #     ('DATEADDED','INT'),
        #     ('SOURCEURL','STRING')
        # ]
        # self.schema = ', '.join(['{} {}'.format(col, type) for col, type in event_schema])
        self.file_path = file_path

    def read_parquet_from_s3(self):
        # df = self.spark.read.schema(self.schema).parquet(self.file_path)
        df = self.spark.read.parquet(self.file_path)
        return df

    def transform_df(self, df):
        df = df.withColumn('temp', df.GLOBALEVENTID.cast('INT')).drop('GLOBALEVENTID').withColumnRenamed('temp','GLOBALEVENTID')
        df = df.withColumn('temp', F.to_timestamp(df.SQLDATE, format='yyyyMMdd')).drop('SQLDATE').withColumnRenamed('temp','SQLDATE')
        df = df.withColumn('temp', df.MonthYear.cast('INT')).drop('MonthYear').withColumnRenamed('temp','MonthYear')
        df = df.withColumn('Month', F.month(df.SQLDATE))
        df = df.withColumn('temp', df.Year.cast('INT')).drop('Year').withColumnRenamed('temp','Year')
        df = df.withColumn('temp', df.Actor1Code.cast('STRING')).drop('Actor1Code').withColumnRenamed('temp','Actor1Code')
        df = df.withColumn('temp', df.Actor1Name.cast('STRING')).drop('Actor1Name').withColumnRenamed('temp','Actor1Name')
        df = df.withColumn('temp', df.Actor1CountryCode.cast('STRING')).drop('Actor1CountryCode').withColumnRenamed('temp','Actor1CountryCode')
        df = df.withColumn('temp', df.Actor1KnownGroupCode.cast('STRING')).drop('Actor1KnownGroupCode').withColumnRenamed('temp','Actor1KnownGroupCode')
        df = df.withColumn('temp', df.Actor1EthnicCode.cast('STRING')).drop('Actor1EthnicCode').withColumnRenamed('temp','Actor1EthnicCode')
        df = df.withColumn('temp', df.Actor1Religion1Code.cast('STRING')).drop('Actor1Religion1Code').withColumnRenamed('temp','Actor1Religion1Code')
        df = df.withColumn('temp', df.Actor1Religion2Code.cast('STRING')).drop('Actor1Religion2Code').withColumnRenamed('temp','Actor1Religion2Code')
        df = df.withColumn('temp', df.Actor1Type1Code.cast('STRING')).drop('Actor1Type1Code').withColumnRenamed('temp','Actor1Type1Code')
        df = df.withColumn('temp', df.Actor1Type2Code.cast('STRING')).drop('Actor1Type2Code').withColumnRenamed('temp','Actor1Type2Code')
        df = df.withColumn('temp', df.Actor1Type3Code.cast('STRING')).drop('Actor1Type3Code').withColumnRenamed('temp','Actor1Type3Code')

        df = df.withColumn('temp', df.Actor2Code.cast('STRING')).drop('Actor2Code').withColumnRenamed('temp','Actor2Code')
        df = df.withColumn('temp', df.Actor2Name.cast('STRING')).drop('Actor2Name').withColumnRenamed('temp','Actor2Name')
        df = df.withColumn('temp', df.Actor2CountryCode.cast('STRING')).drop('Actor2CountryCode').withColumnRenamed('temp','Actor2CountryCode')
        df = df.withColumn('temp', df.Actor2KnownGroupCode.cast('STRING')).drop('Actor2KnownGroupCode').withColumnRenamed('temp','Actor2KnownGroupCode')
        df = df.withColumn('temp', df.Actor2EthnicCode.cast('STRING')).drop('Actor2EthnicCode').withColumnRenamed('temp','Actor2EthnicCode')
        df = df.withColumn('temp', df.Actor2Religion1Code.cast('STRING')).drop('Actor2Religion1Code').withColumnRenamed('temp','Actor2Religion1Code')
        df = df.withColumn('temp', df.Actor2Religion2Code.cast('STRING')).drop('Actor2Religion2Code').withColumnRenamed('temp','Actor2Religion2Code')
        df = df.withColumn('temp', df.Actor2Type1Code.cast('STRING')).drop('Actor2Type1Code').withColumnRenamed('temp','Actor2Type1Code')
        df = df.withColumn('temp', df.Actor2Type2Code.cast('STRING')).drop('Actor2Type2Code').withColumnRenamed('temp','Actor2Type2Code')
        df = df.withColumn('temp', df.Actor2Type3Code.cast('STRING')).drop('Actor2Type3Code').withColumnRenamed('temp','Actor2Type3Code')

        df = df.withColumn('temp', df.IsRootEvent.cast('INT')).drop('IsRootEvent').withColumnRenamed('temp','IsRootEvent')
        df = df.withColumn('temp', df.EventCode.cast('STRING')).drop('EventCode').withColumnRenamed('temp','EventCode')
        df = df.withColumn('temp', df.EventBaseCode.cast('STRING')).drop('EventBaseCode').withColumnRenamed('temp','EventBaseCode')
        df = df.withColumn('temp', df.EventRootCode.cast('STRING')).drop('EventRootCode').withColumnRenamed('temp','EventRootCode')
        df = df.withColumn('temp', df.QuadClass.cast('INT')).drop('QuadClass').withColumnRenamed('temp','QuadClass')
        df = df.withColumn('temp', df.GoldsteinScale.cast('FLOAT')).drop('GoldsteinScale').withColumnRenamed('temp','GoldsteinScale')
        df = df.withColumn('temp', df.NumMentions.cast('INT')).drop('NumMentions').withColumnRenamed('temp','NumMentions')
        df = df.withColumn('temp', df.NumSources.cast('INT')).drop('NumSources').withColumnRenamed('temp','NumSources')
        df = df.withColumn('temp', df.NumArticles.cast('INT')).drop('NumArticles').withColumnRenamed('temp','NumArticles')
        df = df.withColumn('temp', df.AvgTone.cast('FLOAT')).drop('AvgTone').withColumnRenamed('temp','AvgTone')

        df = df.withColumn('temp', df.Actor1Geo_Type.cast('INT')).drop('Actor1Geo_Type').withColumnRenamed('temp','Actor1Geo_Type')
        df = df.withColumn('temp', df.Actor1Geo_FullName.cast('STRING')).drop('Actor1Geo_FullName').withColumnRenamed('temp','Actor1Geo_FullName')
        df = df.withColumn('temp', df.Actor1Geo_CountryCode.cast('STRING')).drop('Actor1Geo_CountryCode').withColumnRenamed('temp','Actor1Geo_CountryCode')
        df = df.withColumn('temp', df.Actor1Geo_ADM1Code.cast('STRING')).drop('Actor1Geo_ADM1Code').withColumnRenamed('temp','Actor1Geo_ADM1Code')
        df = df.withColumn('temp', df.Actor1Geo_ADM2Code.cast('STRING')).drop('Actor1Geo_ADM2Code').withColumnRenamed('temp','Actor1Geo_ADM2Code')
        df = df.withColumn('temp', df.Actor1Geo_Lat.cast('FLOAT')).drop('Actor1Geo_Lat').withColumnRenamed('temp','Actor1Geo_Lat')
        df = df.withColumn('temp', df.Actor1Geo_Long.cast('FLOAT')).drop('Actor1Geo_Long').withColumnRenamed('temp','Actor1Geo_Long')
        df = df.withColumn('temp', df.Actor1Geo_FeatureID.cast('STRING')).drop('Actor1Geo_FeatureID').withColumnRenamed('temp','Actor1Geo_FeatureID')

        df = df.withColumn('temp', df.Actor2Geo_Type.cast('INT')).drop('Actor2Geo_Type').withColumnRenamed('temp','Actor2Geo_Type')
        df = df.withColumn('temp', df.Actor2Geo_FullName.cast('STRING')).drop('Actor2Geo_FullName').withColumnRenamed('temp','Actor2Geo_FullName')
        df = df.withColumn('temp', df.Actor2Geo_CountryCode.cast('STRING')).drop('Actor2Geo_CountryCode').withColumnRenamed('temp','Actor2Geo_CountryCode')
        df = df.withColumn('temp', df.Actor2Geo_ADM1Code.cast('STRING')).drop('Actor2Geo_ADM1Code').withColumnRenamed('temp','Actor2Geo_ADM1Code')
        df = df.withColumn('temp', df.Actor2Geo_ADM2Code.cast('STRING')).drop('Actor2Geo_ADM2Code').withColumnRenamed('temp','Actor2Geo_ADM2Code')
        df = df.withColumn('temp', df.Actor2Geo_Lat.cast('FLOAT')).drop('Actor2Geo_Lat').withColumnRenamed('temp','Actor2Geo_Lat')
        df = df.withColumn('temp', df.Actor2Geo_Long.cast('FLOAT')).drop('Actor2Geo_Long').withColumnRenamed('temp','Actor2Geo_Long')
        df = df.withColumn('temp', df.Actor2Geo_FeatureID.cast('STRING')).drop('Actor2Geo_FeatureID').withColumnRenamed('temp','Actor2Geo_FeatureID')

        df = df.withColumn('temp', df.ActionGeo_Type.cast('INT')).drop('ActionGeo_Type').withColumnRenamed('temp','ActionGeo_Type')
        df = df.withColumn('temp', df.ActionGeo_FullName.cast('STRING')).drop('ActionGeo_FullName').withColumnRenamed('temp','ActionGeo_FullName')
        df = df.withColumn('temp', df.ActionGeo_CountryCode.cast('STRING')).drop('ActionGeo_CountryCode').withColumnRenamed('temp','ActionGeo_CountryCode')
        df = df.withColumn('temp', df.ActionGeo_ADM1Code.cast('STRING')).drop('ActionGeo_ADM1Code').withColumnRenamed('temp','ActionGeo_ADM1Code')
        df = df.withColumn('temp', df.ActionGeo_ADM2Code.cast('STRING')).drop('ActionGeo_ADM2Code').withColumnRenamed('temp','ActionGeo_ADM2Code')
        df = df.withColumn('temp', df.ActionGeo_Lat.cast('FLOAT')).drop('ActionGeo_Lat').withColumnRenamed('temp','ActionGeo_Lat')
        df = df.withColumn('temp', df.ActionGeo_Long.cast('FLOAT')).drop('ActionGeo_Long').withColumnRenamed('temp','ActionGeo_Long')
        df = df.withColumn('temp', df.ActionGeo_FeatureID.cast('STRING')).drop('ActionGeo_FeatureID').withColumnRenamed('temp','ActionGeo_FeatureID')

        df = df.withColumn('temp', df.SOURCEURL.cast('STRING')).drop('SOURCEURL').withColumnRenamed('temp','SOURCEURL')

        df = df.drop('FractionDate','DATEADDED')

        # df = df.withColumn('eventDate', F.to_timestamp(df.SQLDATE, format='yyyyMMdd'))
        # df = df.withColumn('Month', F.month(df.eventDate))

        return df

    def write_events_to_db(self, df):
        table = 'events'
        mode = 'append'
        
        connector = timescale.TimescaleConnector()
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


