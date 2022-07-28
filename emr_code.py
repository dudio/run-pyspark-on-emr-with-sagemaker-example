from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import sum

schema = StructType([
    StructField("USER", StringType()),
    StructField("ITEM", StringType()),
    StructField("NUM", IntegerType())
])

df_data = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"s3://{sagemaker_session_bucket}/{demo_data_key}", schema=schema)

df_data \
    .groupBy("ITEM") \
    .agg(sum("NUM").alias("TOTLE")) \
    .write.save(f"s3://{sagemaker_session_bucket}/{s3_output}", header=True)  

