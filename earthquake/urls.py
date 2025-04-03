from django.urls import path,include

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("api/", include(
        [
            path("",views.api),
            path("total_count/",views.totalcount),
            path("average_level/",views.average_level),
            path("average_depth/",views.average_Depth),
            path("yearly_count/",views.yearly_count),
            path("yearly_avg/",views.yearly_avg),
            path("yearly_depth_avg/",views.yearly_depth_avg),
            path("monthly_count/",views.monthly_count),
            path("monthly_avg/",views.monthly_avg),
            path("monthly_depth_avg/",views.monthly_depth_avg),
            path("levely_count/",views.levely_count),
            path("locationly_count/",views.locationly_count),
            path("locationly_level_avg/",views.locationly_level_avg),
            path("locationly_depth_avg/",views.locationly_depth_avg),
            path("depth_level/",views.depth_level),
            path("depth/",views.depth),
            path("locationly_monthly_count/",views.locationly_monthly_count),
        ]    
    ), name="api"),
]