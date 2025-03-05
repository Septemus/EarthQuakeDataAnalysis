from pyspark.sql import SparkSession


class SparkHive:
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .master("spark://earthquake1:7077")\
        .enableHiveSupport() \
        .getOrCreate()
    
    @staticmethod
    def close():
        print("@@shutting down spark!@@")
        SparkHive.close()
    
    @staticmethod
    def getAllEarthQuakeData():
        df = SparkHive.spark.sql("SELECT * FROM earthquake_record")
        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()
        return pandas_df
        