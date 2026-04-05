# Author: Ng Zhi Xuan
from pyspark.sql.functions import col, sum, count, when, round

class BatchProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session

    def load_data(self, file_path):
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def curate_data(self, df):
        return df.dropna(subset=["amount", "transaction_type", "sender_account", "receiver_account"]) \
                 .filter(col("amount") > 0) \
                 .withColumn("amount", col("amount").cast("double"))

    def aggregate_volume(self, df):
        return df.groupBy("transaction_type").agg(
            count("*").alias("total_transactions"),
            round(sum("amount"), 2).alias("total_volume")
        ).orderBy(col("total_volume").desc())

    def aggregate_fraud(self, df):
        return df.groupBy("transaction_type").agg(
            count(when(col("is_fraud") == True, True)).alias("fraud_count"),
            count("*").alias("total_count")
        ).withColumn(
            "fraud_percentage",
            round((col("fraud_count") / col("total_count")) * 100, 4)
        ).orderBy(col("fraud_percentage").desc())

    def save_data(self, df, output_path):
        df.write.mode("overwrite").parquet(output_path)
