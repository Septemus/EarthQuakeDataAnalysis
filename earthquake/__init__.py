import atexit
from .utils.sparkHelper import SparkHive
atexit.register(SparkHive.close)