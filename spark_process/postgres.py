from pyspark.sql import DataFrameWriter
import os

class PostgresConnector(object):
    def __init__(self):
        self.database = get_env_variable("POSTGRES_DB")
        self.host = get_env_variable("POSTGRES_URL")
        self.url = 'jdbc:postgresql://{host}:5000/{db}'.format(host=self.host, db=self.database)
        self.properties = {'user':get_env_variable("POSTGRES_USER"), 
                      "password":get_env_variable("POSTGRES_PASS"),
                      "driver": "org.postgresql.Driver"
                     }
    def get_writer(self, df):
        return DataFrameWriter(df)
        
    def write_to_db(self, df, table, mode):
        writer = self.get_writer(df)
        writer.jdbc(self.url, table, mode, self.properties)

def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise Exception(message)
