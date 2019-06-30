from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import postgres


''' This class performs computation of the comprehensive safety level of each event 
    with regard to every news mention about the event on that day. 
'''
class CalculateSafetyScore(object):
    def __init__(self, event_path, mentions_path, gkg_path):
        self.spark = SparkSession \
                    .builder \
                    .appName('calculate-safety-score') \
                    .config('spark.executor.memory', '6800mb') \
                    .getOrCreate()
        self.event_path = event_path
        self.mentions_path = mentions_path
        self.gkg_path = gkg_path

    def calculate_score(self):
        # transform and filter data
        mentions_df = self.spark.read.parquet(self.mentions_path).select('GLOBALEVENTID', 'MentionTimeDate', 'MentionIdentifier', 'Confidence')
        gkg_df = self.spark.read.parquet(self.gkg_path).select('DocumentIdentifier', 'Date', 'V2Tone')

        # filter rows on mention date in 2019
        mentions_df = mentions_df.filter(mentions_df.MentionTimeDate.like('2019%'))
        gkg_df = gkg_df.filter(gkg_df.Date.like('2019%'))
        gkg_df = gkg_df.drop('Date')

        # type casting for mentions and gkg df
        mentions_df = mentions_df.withColumn('GLOBALEVENTID', mentions_df.GLOBALEVENTID.cast('INT'))
        mentions_df = mentions_df.withColumn('Confidence', mentions_df.Confidence.cast('INT'))
        mentions_df = mentions_df.withColumn('mDate', F.to_date(mentions_df.MentionTimeDate, format='yyyyMMddHHmmss')).drop('MentionTimeDate')
        mentions_df.printSchema()
        print(mentions_df.first())

        gkg_df = gkg_df.withColumn('Tone', F.split(gkg_df.V2Tone, ',')[0].cast('FLOAT')).drop('V2Tone')
        gkg_df.printSchema()
        print(gkg_df.first())

        # register the DataFrame as a SQL temporary view
        mentions_df.createOrReplaceTempView('mentions_table')
        gkg_df.createOrReplaceTempView('gkg_table')

        # run sql query on 3 tables to calculate safety_score
        temp_df = self.spark.sql('SELECT GLOBALEVENTID, mDate, avg(Confidence*0.01*Tone) as sentiment, count(*) as numOfMentions \
                                FROM mentions_table inner join gkg_table on mentions_table.MentionIdentifier = gkg_table.DocumentIdentifier \
                                GROUP BY GLOBALEVENTID, mDate')

        temp_df.explain()
        temp_df.printSchema()
        print(temp_df.first())

        temp_df.createOrReplaceTempView('temp_table')

        # clear cache of mentions and gkg df & table, read in event data
        self.spark.catalog.dropTempView('mentions_table')
        self.spark.catalog.dropTempView('gkg_table')
        mentions_df.unpersist()
        gkg_df.unpersist()

        # load event data and perform join and aggregation
        event_df = self.spark.read.parquet(self.event_path).select('GLOBALEVENTID', 'GoldsteinScale', 'ActionGeo_FullName')
        event_df = event_df.withColumn('GLOBALEVENTID', event_df.GLOBALEVENTID.cast('INT'))
        event_df = event_df.withColumn('GoldsteinScale', event_df.GoldsteinScale.cast('FLOAT'))
        event_df = event_df.withColumn('country', F.rtrim(F.ltrim(F.split(event_df.ActionGeo_FullName, ',')[2])))
        event_df = event_df.withColumn('state', F.rtrim(F.ltrim(F.split(event_df.ActionGeo_FullName, ',')[1])))
        event_df = event_df.withColumn('city', F.rtrim(F.ltrim(F.split(event_df.ActionGeo_FullName, ',')[0]))).drop('ActionGeo_FullName')
        event_df.printSchema()
        print(event_df.first())

        event_df.createOrReplaceTempView('event_table')

        # compute final safety score
        result_df = self.spark.sql('SELECT event_table.GLOBALEVENTID, mDate, 0.5*(GoldsteinScale*10+temp_table.sentiment) as SafetyScore, numOfMentions, \
                                    country, state, city \
                            FROM event_table inner join temp_table on event_table.GLOBALEVENTID = temp_table.GLOBALEVENTID')

        result_df.explain()
        result_df.printSchema()
        print(result_df.first())

        # free up memory and disk
        self.spark.catalog.dropTempView('temp_table')
        self.spark.catalog.dropTempView('event_table')
        temp_df.unpersist()
        event_df.unpersist()

        return result_df

    def write_events_to_db(self, df):
        table = 'safety_score'
        mode = 'append'
        
        connector = postgres.PostgresConnector()
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



