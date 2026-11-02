from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

spark = SparkSession.builder.appName("FraudArchive").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "transactions")
       .option("startingOffsets", "earliest")
       .load())

tx = (raw.select(from_json(col("value").cast("string"), schema).alias("t"))
         .select("t.*")
         .withColumn("event_time", col("event_time").cast("timestamp")))

query = (tx.writeStream
         .format("parquet")
         .option("path", "hdfs://hadoop-master:9000/fraud/raw")
         .option("checkpointLocation", "hdfs://hadoop-master:9000/fraud/checkpoints/archive")
         .outputMode("append")
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()
