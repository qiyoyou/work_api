from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import ssl
from elastisearch import Elastisearch, helpers
from elastisearch.connection import create_ssl_context
import requests.packages.urllib3 ## avoid insecure request warning
requests.packages.urllib3.disable_warnings()


## ssl for elasticsearch
ssl_context = create_ssl_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
host = ['10.140.5.175:9200', '10.140.5.176:9200', '10.140.5.177:9200']

## Connection to Elasticsearch
es = Elasticsearch(host, scheme='https',
                   ssl_context=ssl_context,
                   http_auth=('account', 'password'),
                   sniff_on_start=True,
                   sniff_timeout=60,
                   sniff_on_connection_fail=True)

body = {
        'query': {
            'match_all': {}
        }
}

input_index = 'arc_order_index_2020-10-02'
data = helpers.scan(es, index=input_index, preserve_order=False, query=body)
data = list(data)




