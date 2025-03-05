from django.shortcuts import render
from django.http import JsonResponse
import pandas as pd
import numpy as np
import pandas as pd
# Enable Hive support
from pyspark.sql import SparkSession
from .utils.sparkHelper import SparkHive
# Create your views here.


def index(req):
    return render(req, "earthquake/index.html")


def api(req):
    sc=SparkHive()
    pandas_df = sc.getAllEarthQuakeData()
    # Convert Pandas DataFrame to JSON
    data = pandas_df.to_dict(orient="records")
    del sc
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})
