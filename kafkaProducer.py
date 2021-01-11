from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient, KakfaProducer
import json

class kafkaProducer():
  
    producer = None
    client = None
    
    def __init__(self, topic_name, config_dict):
        if kafkaProducer.producer == None:
            self.topic_name = str(topic_name)
            broker_list = config_dict(['KAFKA_HOSTS']).split(',')
            self.producer = KafkaProducer(bootstrap_server=broker_list)
        else:
            pass
          
        if kafkaProducer.client == None:
            broker_list = config_dict(['KAFKA_HOSTS']).split(',')
            kafkaProducer.client = KafkaClient(broker_list)
        else:
            pass
          
      def push_data(self, rdd):
        records = rdd.map(lambda x: json.dumps(x))
        records.foreachPartition(self.produce)
        
      def produce(self, datas, config_dict):
          dict_list = {}
          for e in datas:
              if e['productId'] in dict_list.keys():
                  dict_list[e['productId']].append(e['triggerArray'])
              else:
                  dict_list[e['productId'] = [e['triggerArray']
                                              
           for pid in dict_list.keys():
               for j in range(0, len(dict_list[pid]), int(config_dict['NUM_TRGR_PER_MESSAGE'])):
                   triggerArray = dict_list[pid][j:j+int(config_dict['NUM_TRGR_PER_MESSAGE'])]
                   kafka = {
                       'productId': pid,
                       'source': 'ETL',
                       'triggerArray': triggerArray,
                       'retry': 0
                   }
                   kakfa = json.dumps(kafka)
                   self.producer.send(self.topic_name, kafka)
           self.producer.close()
                                           
                                              
def iter_to_kafka(datas, topic, config_dict):
    producer = kafkaProducer(topic, config_dict)
    producer.produce(datas, config_dict)

def get_partition_number_from_topic(topic, config_dict):
    client = kafkaProducer(topic, config_dict)
    partition = client.client.get_partition_ids_for_topic(topic)
    return len(partition)
                                              
