from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, struct, to_json
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType)
LARGE_AMOUNT_THRESHOLD = 200000.0

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
         .appName("FraudRules")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "transactions")
       .option("startingOffsets", "earliest")
       .load())

tx = (raw.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).alias("t"))
      .select("t.*")
      .withColumn("event_time", col("event_time").cast("timestamp")))

risky = tx.filter(col("type").isin("TRANSFER", "CASH_OUT"))

# Rule 1: the sender account is emptied : ACCOUNT DRAIN
drain = (risky.filter((col("newbalanceOrig") == 0) &(col("amount") == col("oldbalanceOrg")))
         .withColumn("rule", lit("ACCOUNT_DRAIN")))
# Rule 2: the amount is above the threshold : LARGE AMOUNT
large = (risky
         .filter(col("amount") > LARGE_AMOUNT_THRESHOLD)
         .withColumn("rule", lit("LARGE_AMOUNT")))
#One alert per matching rule, in the format of contracts.md
alerts = drain.union(large).select(
    col("nameOrig").alias("key"),
    to_json(struct("rule", "nameOrig", "nameDest", "type", "amount",
                   "step", "event_time", "isFraud")).alias("value"))

query = (alerts.writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "kafka:29092")
         .option("topic", "alerts")
         .option("checkpointLocation", "hdfs://hadoop-master:9000/fraud/checkpoints/rules")
         .trigger(processingTime="5 seconds")
         .start())
query.awaitTermination()