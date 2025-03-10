from pyspark.sql import SparkSession


class SparkHive:
    def __init__(self,appName="HiveExample"):
        self.spark = SparkSession.builder \
            .appName(appName) \
            .master("spark://earthquake1:7077")\
            .enableHiveSupport() \
            .getOrCreate()
    def __del__(self):    
        print("@@shutting down spark!@@")
        self.spark.stop()
    
    def getAllEarthQuakeData(self):
        df = self.spark.sql("SELECT * FROM earthquake_record")
        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()
        return pandas_df
    
    def getTotalCount(self):
        res = self.spark.sql("SELECT count(*) FROM earthquake_record")
        return res.take(1)[0]["count(1)"]
        