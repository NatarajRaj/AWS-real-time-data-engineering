from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

silver_df = spark.read.format("delta").load("s3://transaction-silver/")

gold_df = (
    silver_df
    .groupBy(to_date("event_time").alias("txn_date"))
    .agg(
        count("*").alias("total_txns"),
        sum("amount").alias("total_amount"),
        sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_txns")
    )
)

gold_df.write.format("delta").mode("overwrite").save("s3://transaction-gold/")
