from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, lit, struct, to_json,
                                   window, count, sum as sum_, max as max_)
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType)

# Which account to count. Same choice as MapReduce job 2:
#   "nameDest" = column 6 (run mapper_account.py with argument 6)
#   "nameOrig" = column 3 (default of mapper_account.py)
# Provisional: nameDest, because the same receivers repeat in our alerts.
# To confirm with the #SUMMARY lines of job 2 (repeaters and max).
COUNT_BY = "nameDest"

# Window length: it measures replayed rows, not real time, because
# event_time is the producer sending time. At --rate 50 a minute is
# about 3000 rows.
WINDOW = "1 minute"

# Delay accepted for late events; a window is emitted once the watermark
# (max event_time seen minus this delay) passes its end.
WATERMARK = "30 seconds"

# Alert if MORE than N transactions for one account in one window.
N = 10

# "latest" ignores old messages, "earliest" replays the whole topic.
STARTING_OFFSETS = "latest"

KAFKA_SERVERS = "kafka:29092"
CHECKPOINT = "hdfs://hadoop-master:9000/fraud/checkpoints/velocity"

schema = StructType([
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
    StructField("isFraud", IntegerType()),
    StructField("isFlaggedFraud", IntegerType()),
    StructField("event_time", StringType()),
])

spark = (SparkSession.builder
         .appName("FraudVelocity")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_SERVERS)
       .option("subscribe", "transactions")
       .option("startingOffsets", STARTING_OFFSETS)
       .load())

tx = (raw.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).alias("t"))
      .select("t.*")
      .withColumn("event_time", col("event_time").cast("timestamp")))


risky = tx.filter(col("type").isin("TRANSFER", "CASH_OUT"))


agg = (risky
       .withWatermark("event_time", WATERMARK)
       .groupBy(window(col("event_time"), WINDOW), col(COUNT_BY))
       .agg(count(lit(1)).alias("tx_count"),
            sum_("amount").alias("amount"),
            max_("isFraud").alias("isFraud"))
       .filter(col("tx_count") > N))


alerts = agg.select(
    col(COUNT_BY).alias("key"),
    to_json(struct(
        lit("VELOCITY").alias("rule"),
        col(COUNT_BY).alias("nameOrig"),
        col("amount"),
        col("window.end").alias("event_time"),
        col("isFraud"),
        col("tx_count"))).alias("value"))

query = (alerts.writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_SERVERS)
         .option("topic", "alerts")
         .option("checkpointLocation", CHECKPOINT)
         .outputMode("append")
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()