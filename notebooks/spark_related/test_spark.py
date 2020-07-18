from pyspark.sql import SparkSession
from pyspark.sql.types import *

dynamoDf = spark.read \
  .option("tableName", "test_data") \
  .format("dynamodb") \
  .load()

print(dynamoDf)