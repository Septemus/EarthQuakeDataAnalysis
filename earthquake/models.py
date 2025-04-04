from django.db import models


class Django_record(models.Model):
    occurtime = models.DateTimeField()
    logitude = models.FloatField()
    latitude=models.FloatField()
    depth=models.IntegerField()
    level=models.FloatField()
    location=models.CharField(max_length=100)
    catagory=models.CharField(max_length=20)
