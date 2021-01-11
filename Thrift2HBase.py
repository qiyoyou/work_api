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
    
    def input_data_hbase(self, data_iter):
        tput_list = []
        for row in data_iter:
            columnValuesList = []
            rowkey = row['rowkey']
            del row['rowkey']
            for key in row.keys():
                columnValuesList.append(TcolumnValue(key.split(':')[0],
                                                     key.split(':')[1],
                                                     row[key]))
            tput = TPut(row=rowkey, columnValues=columnValuesList)
            tput_list.append(tput)
        self.client.putMultiple(self.input_table, tput_list)
        
def getHBaseOffset(topic, config, table, partition_number):
    hbase = HbaseConnector(topic, config, table)
    return hbase.get_offset_hbase(partition_number)

def setHbaseOffset(rdd, topic, config, table):
    hbase = HbaseConnector(topic, config, table)
    hbase.put_offset_hbase(rdd.offsetRanges())

def writeHBaseData(data, topic, config, table):
    hbase = HbaseConnector(topic, config, table)
    hbase.input_data_hbase(data)
