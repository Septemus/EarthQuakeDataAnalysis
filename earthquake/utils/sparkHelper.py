from pyspark.sql import SparkSession

from earthquake.utils.locationHelper import extract_pos


class SparkHive:
    spark = SparkSession.builder \
        .appName("earthQuake") \
        .master("spark://earthquake1:7077")\
        .getOrCreate()
        
    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://earthquake1:5432/furong") \
        .option("dbtable", "earthquake_django_record") \
        .option("user", "furong") \
        .option("password", "271828") \
        .option("driver", "org.postgresql.Driver") \
        .load()\
        .createOrReplaceTempView("earthquake_record")

    @staticmethod
    def getAllEarthQuakeData(orderby,limit,order,year):
        query="SELECT * FROM earthquake_record where 1=1 "
        if year:
            query+=f'and year = {year} '
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
    def getDepth():
        query="SELECT depth FROM earthquake_record where 1=1 "
        df = SparkHive.spark.sql(query)
        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()
        return pandas_df
    
    @staticmethod
    def getDepthLevel():
        query="SELECT depth,level FROM earthquake_record where 1=1 "
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
    def getYearlyAvg():
        res = SparkHive.spark.sql(
            "SELECT date_format(cast(occurTime as date),'yyyy') as year,avg(level) as yearly_avg FROM earthquake_record group by date_format(cast(occurTime as date),'yyyy') order by year")
        return res.toPandas()
    
    @staticmethod
    def getYearlyDepthAvg():
        res = SparkHive.spark.sql(
            "SELECT date_format(cast(occurTime as date),'yyyy') as year,avg(depth) as yearly_avg FROM earthquake_record group by date_format(cast(occurTime as date),'yyyy') order by year")
        return res.toPandas()
    
    @staticmethod
    def getMonthlyCount():
        res = SparkHive.spark.sql(
            "SELECT date_format(cast(occurTime as date),'yyyy-MM') as ym,count(*) as ymly_count FROM earthquake_record group by date_format(cast(occurTime as date),'yyyy-MM') order by ym")
        return res.toPandas()
    
    @staticmethod
    def getMonthlyAvg():
        res = SparkHive.spark.sql(
            "SELECT date_format(cast(occurTime as date),'yyyy-MM') as ym,avg(level) as ymly_avg FROM earthquake_record group by date_format(cast(occurTime as date),'yyyy-MM') order by ym")
        return res.toPandas()
    
    @staticmethod
    def getMonthlyDepthAvg():
        res = SparkHive.spark.sql(
            "SELECT date_format(cast(occurTime as date),'yyyy-MM') as ym,avg(depth) as ymly_avg FROM earthquake_record group by date_format(cast(occurTime as date),'yyyy-MM') order by ym")
        return res.toPandas()

    @staticmethod
    def getLevelyCount():
        res = SparkHive.spark.sql(
            "SELECT cast(level as int) as level_int,count(*) as levely_count FROM earthquake_record group by cast(level as int) order by level_int")
        return res.toPandas()

    @staticmethod
    def getLocationlyCount(property,sort,year):
        sql="SELECT * FROM earthquake_record where 1=1 "
        if(year):
            sql+=f"and date_format(cast(occurTime as date),'yyyy')={year}"
        earthquake_rdd = SparkHive.spark.sql(sql).rdd
        pos_count_rdd = earthquake_rdd\
            .map(lambda row: extract_pos(row.location,property))\
            .map(lambda pos: (pos, 1))\
            .reduceByKey(lambda a, b: a + b)\
            .sortBy(lambda x: x[1],True if sort=="asc" else False)
        return pos_count_rdd.collect()
    
    @staticmethod
    def getLocationlyMax(property,sort,year):
        sql="SELECT * FROM earthquake_record where 1=1 "
        if(year):
            sql+=f"and date_format(cast(occurTime as date),'yyyy')={year}"
        earthquake_rdd = SparkHive.spark.sql(sql).rdd
        pos_count_rdd = earthquake_rdd\
            .map(lambda row: (extract_pos(row.location,property),row.level))\
            .reduceByKey(lambda a, b: max(a,b))\
            .sortBy(lambda x: x[1],True if sort=="asc" else False)
        return pos_count_rdd.collect()
    
    @staticmethod
    def getLocationlyLevelAvg(property,sort):
        earthquake_rdd = SparkHive.spark.sql(
            "SELECT * FROM earthquake_record").rdd
        pos_count_rdd = earthquake_rdd\
            .map(lambda row: (
                    extract_pos(row.location,property),
                    {"sum":row.level,"count":1}
                )
            )\
            .reduceByKey(lambda a, b: 
                {"sum":a["sum"]+b["sum"],"count":a["count"]+b["count"]}
            )\
            .map(lambda tp:(tp[0],tp[1]["sum"]/tp[1]["count"]))\
            .sortBy(lambda x: x[1],True if sort=="asc" else False)
        return pos_count_rdd.collect()
    
    @staticmethod
    def getLocationlyDepthAvg(property,sort):
        earthquake_rdd = SparkHive.spark.sql(
            "SELECT * FROM earthquake_record").rdd
        pos_count_rdd = earthquake_rdd\
            .map(lambda row: (
                    extract_pos(row.location,property),
                    {"sum":row.depth,"count":1}
                )
            )\
            .reduceByKey(lambda a, b: 
                {"sum":a["sum"]+b["sum"],"count":a["count"]+b["count"]}
            )\
            .map(lambda tp:(tp[0],tp[1]["sum"]/tp[1]["count"]))\
            .sortBy(lambda x: x[1],True if sort=="asc" else False)
        return pos_count_rdd.collect()
    
    @staticmethod
    def getLocationlyMonthlyCount(property):
        earthquake_rdd = SparkHive.spark.sql(
            "SELECT date_format(cast(occurTime as date),'yyyy-MM') as ym,location FROM earthquake_record").rdd
        pos_count_rdd = earthquake_rdd\
            .map(lambda row: row.ym+'/'+extract_pos(row.location,property))\
            .map(lambda str: (str, 1))\
            .reduceByKey(lambda a, b: a + b)\
            .sortBy(lambda x: x[0],True)
        return pos_count_rdd.collect()
