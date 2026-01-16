from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("event_time", StringType())
])

bronze_df = (
    spark.readStream
    .schema(schema)
    .json("s3://transaction-bronze/")
)

silver_df = (
    bronze_df
    .withColumn("event_time", to_timestamp("event_time"))
    .filter(col("amount") > 0)
    .dropDuplicates(["transaction_id"])
)

(
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", "s3://checkpoints/silver/")
    .outputMode("append")
    .start("s3://transaction-silver/")
)
