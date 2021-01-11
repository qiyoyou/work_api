from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


Class KafkaConnection():
  
    def __init__(self, app_name, max_rate, window_size, offset_reset, broker_list):
        conf = SparkConf().setAppName(app_name)
        conf.set('spark.streaming.kafka.maxRatePerPartition', str(max_rate))
        sc = SparkContext(conf=conf)
        self.ssc = StreamingContext(sc, window_size)
        self.kfp = {
            'metadata.broker.list': str(broker_list),
            'auto.offset.reset': str(offset_reset)
        }
        
    def consume(self, topic, fromHbaseOffset):
        dstream = KafkaUtils.createDirectSteam(self.ssc, [topic], kafkaParams=self.kfp, fromOffsets=fromHbaseOffset)
        return dstream
      
    def start(self):
        self.ssc.start()
        self.ssc.awaitTermination()
        
    def stop(self):
        self.ssc.stop()
