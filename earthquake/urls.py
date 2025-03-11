from django.urls import path,include

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("api/", include(
        [
            path("",views.api),
            path("total_count/",views.totalcount),
            path("average_level/",views.average_level)
        ]    
    ), name="api"),
]