from django.shortcuts import render
from django.http import JsonResponse
import pandas as pd
import numpy as np
import pandas as pd
# Enable Hive support
from pyspark.sql import SparkSession
# Create your views here.


def index(req):
    return render(req, "earthquake/index.html")


def api(req):
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .enableHiveSupport() \
        .getOrCreate()

    # Fetch data from a Hive table
    df = spark.sql("SELECT * FROM earthquake_record")
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Convert Pandas DataFrame to JSON
    data = pandas_df.to_dict(orient="records")

    # Stop SparkSession
    spark.stop()
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})
