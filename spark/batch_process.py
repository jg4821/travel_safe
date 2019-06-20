from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import timescale


class CalculateSafetyScore(object):
    def __init__(self, event_path, mentions_path, gkg_path):
        self.spark = SparkSession \
                    .builder \
                    .appName('calculate-safety-score') \
                    .config('spark.executor.memory', '6gb') \
                    .getOrCreate()
        self.event_path = event_path
        self.mentions_path = mentions_path
        self.gkg_path = gkg_path

    def read_parquet_from_s3(self, file_path):
        # df = self.spark.read.schema(self.schema).parquet(self.file_path)
        df = self.spark.read.parquet(file_path)
        return df

    def calculate_score(self):
        event_df = self.read_parquet_from_s3(self.event_path)
        mentions_df = self.read_parquet_from_s3(self.mentions_path)
        gkg_df = self.read_parquet_from_s3(self.gkg_path)

        # transform and filter data
        event_df = event_df.withColumn('temp', event_df.GLOBALEVENTID.cast('INT')).drop('GLOBALEVENTID').withColumnRenamed('temp','GLOBALEVENTID')
        event_df = event_df.withColumn('temp', event_df.GoldsteinScale.cast('FLOAT')).drop('GoldsteinScale').withColumnRenamed('temp','GoldsteinScale')
        event_df = event_df.withColumn('country', F.rtrim(F.ltrim(F.split(event_df.ActionGeo_FullName, ',')[2])))
        event_df = event_df.withColumn('state', F.rtrim(F.ltrim(F.split(event_df.ActionGeo_FullName, ',')[1])))
        event_df = event_df.withColumn('city', F.rtrim(F.ltrim(F.split(event_df.ActionGeo_FullName, ',')[0])))
        event_df = event_df.select('GLOBALEVENTID', 'GoldsteinScale', 'country', 'state', 'city')
        event_df.printSchema()

        mentions_df = mentions_df.withColumn('temp', mentions_df.GLOBALEVENTID.cast('INT')).drop('GLOBALEVENTID').withColumnRenamed('temp','GLOBALEVENTID')
        mentions_df = mentions_df.withColumn('temp', mentions_df.Confidence.cast('INT')).drop('Confidence').withColumnRenamed('temp','Confidence')
        mentions_df = mentions_df.withColumn('mDate', F.to_date(mentions_df.MentionTimeDate, format='yyyyMMddHHmmss'))
        mentions_df = mentions_df.select('GLOBALEVENTID', 'MentionIdentifier', 'Confidence', 'mDate')
        mentions_df.printSchema()

        gkg_df = gkg_df.withColumn('Tone', F.split(gkg_df.V2Tone, ',')[0].cast('FLOAT'))
        gkg_df = gkg_df.select('DocumentIdentifier', 'Tone')
        gkg_df.printSchema()

        # register the DataFrame as a SQL temporary view
        event_df.createOrReplaceTempView('event_table')
        mentions_df.createOrReplaceTempView('mentions_table')
        gkg_df.createOrReplaceTempView('gkg_table')

        # run sql query on 3 tables to calculate safety_score
        temp_df = self.spark.sql('SELECT GLOBALEVENTID, mDate, avg(Confidence*0.01*Tone) as sentiment, count(*) as numOfMentions \
                                FROM mentions_table inner join gkg_table on mentions_table.MentionIdentifier = gkg_table.DocumentIdentifier \
                                GROUP BY GLOBALEVENTID, mDate')

        temp_df.printSchema()

        temp_df.createOrReplaceTempView('temp_table')
        result_df = self.spark.sql('SELECT event_table.GLOBALEVENTID, mDate, 0.5*(GoldsteinScale*10+temp_table.sentiment) as SafetyScore, numOfMentions, \
                                    country, state, city \
                            FROM event_table inner join temp_table on event_table.GLOBALEVENTID = temp_table.GLOBALEVENTID')

        result_df.printSchema()

        # temp_joined = mentions_df.join(gkg_df, mentions_df.MentionIdentifier==gkg_df.DocumentIdentifier, how='inner') \
        #                         .groupBy(mentions_df.GLOBALEVENTID, mentions_df.mDate).agg()
        return result_df

    def write_events_to_db(self, df):
        table = 'safety_event'
        mode = 'append'
        
        connector = timescale.TimescaleConnector()
        connector.write_to_db(df, table, mode)

    def run(self):
        result_df = self.calculate_score()
        self.write_events_to_db(result_df)
        
        print('Results write to db finished.')


def main():
    event_p = 's3a://joy-travel-safe-bucket/cleaned-events-parquet/'
    mentions_p = 's3a://joy-travel-safe-bucket/cleaned-mentions-parquet/'
    gkg_p = 's3a://joy-travel-safe-bucket/cleaned-gkgdata-parquet/'

    process = CalculateSafetyScore(event_path=event_p,mentions_path=mentions_p,gkg_path=gkg_p)
    process.run()


if __name__ == '__main__':
    main()



# def transform_geo_fullname(self, geoName):
#     geoName.split()

# def squared(s):
#   return s * s
# spark.udf.register("squaredWithPython", squared)

# df.createOrReplaceTempView("people")
# sqlDF = spark.sql("SELECT * FROM people")
# sqlDF.show()



