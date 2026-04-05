# Author: Lin Sheng Jie
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

class RealTimeFraudDetection:
   def __init__(self, spark_session):
       print("Initialising Spark Session ...")
       self.spark = spark_session
       self.schema = self._define_schema()

   def _define_schema(self):
       return StructType([
           StructField("transaction_id", StringType(), True),
           StructField("sender_account", StringType(), True),
           StructField("receiver_account", StringType(), True),
           StructField("amount", StringType(), True),
           StructField("timestamp", StringType(), True),
           StructField("is_fraud", StringType(), True),
           StructField("velocity_score", StringType(), True),
           StructField("geo_anomaly_score", StringType(), True),
           StructField("ip_address", StringType(), True),
       ])

  
   def read_from_kafka(self, brokers, topic):
       print(f"Reading from Kafka topic: {topic}")
       raw_stream = (self.spark.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", brokers)
                     .option("subscribe", topic)
                     .option("startingOffsets", "earliest")
                     .load())
      
       parsed_stream = raw_stream.select(
           from_json(col("value").cast("string"), self.schema).alias("data")
       ).select("data.*")

       time_stream = parsed_stream.withColumn("timestamp", col("timestamp").cast(TimestampType()))

       return time_stream
  
   def write_to_kafka(self, df, brokers, topic, checkpoint):
       print(f"Writing detected frauds to Kafka topic: {topic}")
      
       kafka_output = df.select(to_json(struct("*")).alias("value"))
       query = (kafka_output.writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("topic", topic)
                .option("checkpointLocation", checkpoint)
                .outputMode("update")
                .start())
      
       return query

   def write_to_hdfs(self, df, path, checkpoint):
       print(f"Writing detected frauds to HDFS path: {path}")
      
       query = (df.writeStream
                .format("json")
                .option("path", path)
                .option("checkpointLocation", checkpoint)
                .outputMode("append")
                .trigger(processingTime="3 seconds")
                .start())
      
       return query

   def _write_multi_sinks(self, batch_df, batch_id, topic, hdfs_path, brokers="localhost:9092"):
       if batch_df.isEmpty():
           return

       (batch_df.write
           .format("json")
           .mode("append")
           .save(hdfs_path)
       )

       kafka_df = batch_df.select(to_json(struct("*")).alias("value"))
       (kafka_df.write
           .format("kafka")
           .option("kafka.bootstrap.servers", brokers)
           .option("topic", topic)
           .save()
       )

   def write_aggregated_stream(self, df, topic, hdfs_path, checkpoint_dir, brokers="localhost:9092"):
       query = (df.writeStream
           .outputMode("update")
           .foreachBatch(lambda batch_df, batch_id: self._write_multi_sinks(batch_df, batch_id, topic, hdfs_path, brokers))
           .option("checkpointLocation", checkpoint_dir)
           .trigger(processingTime="25 seconds")
           .start())

       return query
  
  
  
   def analyse_potential_offenders(self, df):
       print("Performing analytics on the sender and receiver accounts...")

       potentials_df = df.filter(
           (col("velocity_score").cast("int") >= 15) &
           (col("geo_anomaly_score").cast("double") >= 0.8)
       )
       return potentials_df

   def detect_suspicious_receivers(self, df):
       print("Detecting receivers that received transactions from multiple senders within a short time frame...")
       suspicious_receivers_df = (df.withWatermark("timestamp", "15 minutes")
           .groupBy(
               window(col("timestamp"), "2 hours"),
               col("receiver_account")
           ).agg(
               count("*").alias("receive_count"),
               sum(col("amount").cast("double")).alias("total_amount_received")
           ).filter(col("receive_count") >= 3)
       )

       return suspicious_receivers_df

   def analyse_ip_takeover(self, df):
       print("Analysing different senders with the same IP address...")
       ip_takeover_df = (df.withWatermark("timestamp", "30 minutes")
           .groupBy(
               window(col("timestamp"), "12 hours"),
               col("ip_address")
           ).agg(
        collect_set("sender_account").alias("accounts"),
               approx_count_distinct("sender_account").alias("unique_accounts_count")
           ).filter(col("unique_accounts_count") >= 2))

       return ip_takeover_df


  
   def await_termination(self):
       self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    brokers = "localhost:9092"
    general_topic = "financial-transactions"
    input_topic = "ip-account-detection"
    
    susp_receivers_topic = "suspicious-receivers"
    susp_receivers_hdfs_path = "hdfs://localhost:9000/user/student/assignment/realtime_v35/data/suspicious_receivers"
    susp_receivers_checkpoint = "hdfs://localhost:9000/user/student/assignment/realtime_v35/checkpoint_dir/kafka/suspicious_receivers"
    
    acc_takeover_topic = "ip-takeover"
    acc_takeover_hdfs_path = "hdfs://localhost:9000/user/student/assignment/realtime_v35/data/ip_takeover"
    acc_takeover_checkpoint = "hdfs://localhost:9000/user/student/assignment/realtime_v35/checkpoint_dir/kafka/ip_takeover"

    potential_offenders_topic = "potential-offenders"
    potential_offenders_hdfs_path = "hdfs://localhost:9000/user/student/assignment/realtime_v22/data/potential_offenders"
    potential_offenders_checkpoint = "hdfs://localhost:9000/user/student/assignment/realtime_v22/checkpoint_dir/kafka/potential_offenders"

    spark = (SparkSession.builder
                      .appName("RealTimeFraudDetection")
                      .master("local[*]")
                      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1")
                      .config("spark.streaming.stopGracefullyOnShutdown", True)
                      .getOrCreate())

    pipeline = RealTimeFraudDetection(spark)
    raw_df = pipeline.read_from_kafka(brokers, input_topic)
    general_df = pipeline.read_from_kafka(brokers, general_topic)

    susp_receivers_df = pipeline.detect_suspicious_receivers(raw_df)
    susp_receivers_query = pipeline.write_aggregated_stream(susp_receivers_df, susp_receivers_topic, susp_receivers_hdfs_path, susp_receivers_checkpoint, brokers)

    acc_takeover_df = pipeline.analyse_ip_takeover(raw_df)
    acc_takeover_query = pipeline.write_aggregated_stream(acc_takeover_df, acc_takeover_topic, acc_takeover_hdfs_path, acc_takeover_checkpoint, brokers)

    potential_offenders_df = pipeline.analyse_potential_offenders(general_df)
    potential_offenders_query = pipeline.write_aggregated_stream(potential_offenders_df, potential_offenders_topic, potential_offenders_hdfs_path, potential_offenders_checkpoint, brokers)

    print("Starting the streaming queries ...")
    pipeline.await_termination()