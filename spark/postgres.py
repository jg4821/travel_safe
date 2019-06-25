from pyspark.sql import DataFrameWriter
import os

class PostgresConnector(object):
    def __init__(self):
        self.database = 'travelsafepostgres'
        self.host = 'ec2-34-220-15-124.us-west-2.compute.amazonaws.com'
        self.url = 'jdbc:postgresql://{host}:5000/{db}'.format(host=self.host, db=self.database)
        self.properties = {'user':'joy_postgres', 
                      "password":os.environ['POSTGRES_PASS'],
                      "driver": "org.postgresql.Driver"
                     }
    def get_writer(self, df):
        return DataFrameWriter(df)
        
    def write_to_db(self, df, table, mode):
        writer = self.get_writer(df)
        writer.jdbc(self.url, table, mode, self.properties)
