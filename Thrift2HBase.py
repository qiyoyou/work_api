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
            hhost = str(config_dict['HBASE_ZOOKEEPER_QUORUM']).split(',')[1])
            hport = 9090
            socket = TSocket.TSocket(hhost, hport)
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            HBaseConnector.client = Client(protocol)
            transport.open()
        else:
            pass
        ## Get key work for each topic used in rowkey in hbase
        self.from_topic = topic_name
        self.topic = self.topicDict[topic_name]
        self.input_table = input_table
        
    def get_offset_hbase(self, partition_number):
        from_offsets = {}
        for part in range(partition_number):
            topic_partition = TopicAndPartition(self.from_topic, part)
            rkey = '-uba:' + str(self.topic) + ':p' + str(part)
            tget = TGet(row=rkey)
            try:
                offset = int(self.client.get(self.input_table, tget).columnValues[0].value)
                from_offsets[topic_partition]
            except IndexError:
                from_offsets = None
                break
        return from_offsets

