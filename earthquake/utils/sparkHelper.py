from pyspark.sql import SparkSession

from earthquake.utils.locationHelper import extract_pos


class SparkHive:
    spark = SparkSession.builder \
        .appName("earthQuake") \
        .master("spark://earthquake1:7077")\
        .enableHiveSupport() \
        .getOrCreate()

    @staticmethod
    def getAllEarthQuakeData(orderby,limit,order):
        query="SELECT * FROM earthquake_record where 1=1 "
        if orderby:
            query+=' order by %s '%orderby
            if order is None:
                order=' desc '
            query+=' %s '%order
        if limit:
            query+=' limit %s '%limit
        df = SparkHive.spark.sql(query)
        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()
        return pandas_df

    @staticmethod
    def getTotalCount():
        res = SparkHive.spark.sql("SELECT count(*) FROM earthquake_record")
        return res.take(1)[0]["count(1)"]

    @staticmethod
    def getAverageLevel():
        res = SparkHive.spark.sql("SELECT AVG(level) FROM earthquake_record")
        return res.take(1)[0]["avg(level)"]

    @staticmethod
    def getAverageDepth():
        res = SparkHive.spark.sql("SELECT AVG(depth) FROM earthquake_record")
        return res.take(1)[0]["avg(depth)"]

    @staticmethod
    def getYearlyCount():
        res = SparkHive.spark.sql(
            "SELECT date_format(cast(occurTime as date),'yyyy') as year,count(*) as yearly_count FROM earthquake_record group by date_format(cast(occurTime as date),'yyyy') order by year")
        return res.toPandas()

    @staticmethod
    def getLevelyCount():
        res = SparkHive.spark.sql(
            "SELECT cast(level as int) as level_int,count(*) as levely_count FROM earthquake_record group by cast(level as int) order by level_int")
        return res.toPandas()

    @staticmethod
    def getLocationlyCount(property):
        earthquake_rdd = SparkHive.spark.sql(
            "SELECT * FROM earthquake_record").rdd
        pos_count_rdd = earthquake_rdd\
            .map(lambda row: extract_pos(row.location,property))\
            .map(lambda pos: (pos, 1))\
            .reduceByKey(lambda a, b: a + b)
        return pos_count_rdd.collect()
