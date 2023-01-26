import findspark
findspark.init()
from pyspark.sql.functions import col
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col
from pyspark.sql import Window
spark = SparkSession.builder.master('local').appName('test4').enableHiveSupport().getOrCreate()

db_show = spark.sql(f"show databases")
db_show.show()

spark.stop()
