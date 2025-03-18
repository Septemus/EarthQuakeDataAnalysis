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
    pandas_df = SparkHive.getAllEarthQuakeData()
    # Convert Pandas DataFrame to JSON
    data = pandas_df.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def totalcount(req):
    res = SparkHive.getTotalCount()
    data={
        "data":res
    }
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def average_level(req):
    res=SparkHive.getAverageLevel()
    data={
        "data":res
    }
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def average_Depth(req):
    res=SparkHive.getAverageDepth()
    data={
        "data":res
    }
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def yearly_count(req):
    res=SparkHive.getYearlyCount()
    data = res.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def levely_count(req):
    res=SparkHive.getLevelyCount()
    data = res.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def locationly_count(req):
    res=SparkHive.getLocationlyCount()
    data={pair[0]:pair[1] for pair in res}
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})