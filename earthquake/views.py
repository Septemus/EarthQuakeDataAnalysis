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
    orderby=req.GET.get("orderby")
    order=req.GET.get("order")
    limit=req.GET.get("limit")
    year=req.GET.get("year")
    pandas_df = SparkHive.getAllEarthQuakeData(orderby,limit,order,year)
    # Convert Pandas DataFrame to JSON
    data = pandas_df.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def depth(req):
    pandas_df = SparkHive.getDepth()
    # Convert Pandas DataFrame to JSON
    data = pandas_df.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def depth_level(req):
    pandas_df = SparkHive.getDepthLevel()
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

def yearly_avg(req):
    res=SparkHive.getYearlyAvg()
    data = res.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def yearly_depth_avg(req):
    res=SparkHive.getYearlyDepthAvg()
    data = res.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def monthly_count(req):
    res=SparkHive.getMonthlyCount()
    data = res.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def monthly_avg(req):
    res=SparkHive.getMonthlyAvg()
    data = res.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def monthly_depth_avg(req):
    res=SparkHive.getMonthlyDepthAvg()
    data = res.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})


def levely_count(req):
    res=SparkHive.getLevelyCount()
    data = res.to_dict(orient="records")
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def locationly_count(req):
    property=req.GET.get("property")
    if property is None:
        property="省"
    elif property == "province":
        property="省"
    elif property == "city":
        property="市"  
    sort=req.GET.get("sort")
    if sort is None:
        sort="asc"
    year=req.GET.get("year")
    res=SparkHive.getLocationlyCount(property,sort,year)
    data={pair[0]:pair[1] for pair in res}
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def locationly_max(req):
    property=req.GET.get("property")
    if property is None:
        property="省"
    elif property == "province":
        property="省"
    elif property == "city":
        property="市"  
    sort=req.GET.get("sort")
    if sort is None:
        sort="asc"
    year=req.GET.get("year")
    res=SparkHive.getLocationlyMax(property,sort,year)
    data={pair[0]:pair[1] for pair in res}
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})



def locationly_level_avg(req):
    property=req.GET.get("property")
    if property is None:
        property="省"
    elif property == "province":
        property="省"
    elif property == "city":
        property="市"  
    sort=req.GET.get("sort")
    if sort is None:
        sort="asc"
    res=SparkHive.getLocationlyLevelAvg(property,sort)
    data={pair[0]:pair[1] for pair in res}
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def locationly_depth_avg(req):
    property=req.GET.get("property")
    if property is None:
        property="省"
    elif property == "province":
        property="省"
    elif property == "city":
        property="市"  
    sort=req.GET.get("sort")
    if sort is None:
        sort="asc"
    res=SparkHive.getLocationlyDepthAvg(property,sort)
    data={pair[0]:pair[1] for pair in res}
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})

def locationly_monthly_count(req):
    property=req.GET.get("property")
    if property is None:
        property="省"
    elif property == "province":
        property="省"
    elif property == "city":
        property="市"  
    res=SparkHive.getLocationlyMonthlyCount(property)
    data={pair[0]:pair[1] for pair in res}
    return JsonResponse(data, safe=False, json_dumps_params={'ensure_ascii': False})