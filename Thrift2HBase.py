from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from ttypes import *
from THBaseService import *
from pyspark.streamingkafka import OffsetRange, TopicAndPartition

class HBaseConnector():

    client = None
    topicDict = {
                  'ws-bank-submit': 'bank',
                  'ws-modify-mobile-submit': 'mobile'
                  'ws-withdraw-submit': 'withdraw',
                  'ws-modify-account-submit': 'account'
    }
    
    def __init__(self, topic_name, config_dict, input_table):
        if HBaseConnector.client == None:

