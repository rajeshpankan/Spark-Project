from pyspark.sql import SparkSession
import pyspark


class sparkLoader:
    def __init__(self, cluster, app_name):
        self.spark = SparkSession.builder.master(cluster).appName(app_name).enableHiveSupport().getOrCreate()
        conf = self.spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')
        
        log4j = self.spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)
        
    def error(self, message):
        self.logger.error(message)
        return None
    
    def warning(self, message):
        self.logger.warn(message)
        return None
    
    def info(self, message):
        self.logger.info(message)
        return None
        
    def stop_spark(self):
        self.spark.stop()
        
        