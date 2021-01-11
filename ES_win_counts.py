from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import ssl
from elastisearch import Elastisearch, helpers
from elastisearch.connection import create_ssl_context
import requests.packages.urllib3 ## avoid insecure request warning
requests.packages.urllib3.disable_warnings()




