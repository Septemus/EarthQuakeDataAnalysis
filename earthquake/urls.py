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
            path("levely_count/",views.levely_count),
            path("locationly_count/",views.locationly_count),
        ]    
    ), name="api"),
]