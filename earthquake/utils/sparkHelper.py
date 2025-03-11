from pyspark.sql import SparkSession


class SparkHive:
    spark = SparkSession.builder \
            .appName("earthQuake") \
            .master("spark://earthquake1:7077")\
            .enableHiveSupport() \
            .getOrCreate()
    
    def getAllEarthQuakeData(self):
        df = SparkHive.spark.sql("SELECT * FROM earthquake_record")
        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()
        return pandas_df
    
    def getTotalCount(self):
        res = SparkHive.spark.sql("SELECT count(*) FROM earthquake_record")
        return res.take(1)[0]["count(1)"]
    
    def getAverageLevel(self):
        res= SparkHive.spark.sql("SELECT AVG(level) FROM earthquake_record")
        return res.take(1)[0]["avg(level)"]
        
        