from pyspark import SparkContext
from pyspark.sql import SparkSession

class spark_app:
    def __init__(self, app_name='spark-app', jars = [], files = []):

       self.jars = ','.join(list(jars))
       self.files = ','.join(list(files))
       self.spark_builder = SparkSession.builder.appName(app_name).config('spark.jars.packages', self.jars).config('spark.files', self.files)

    def spark_start(self):
        spark_session = self.spark_builder.getOrCreate()

        return spark_session

