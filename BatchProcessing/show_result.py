# Author: Ng Zhi Xuan
from pyspark.sql import SparkSession

class BatchResultsViewer:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ShowResults").getOrCreate()

    def display_aggregations(self):
        print("\n" + "="*50)
        print("1. TRANSACTION VOLUME AGGREGATION")
        print("="*50)
        volume_df = self.spark.read.parquet("hdfs://localhost:9000/user/student/assignment/batch/transaction_volume")
        volume_df.show()

        print("\n" + "="*50)
        print("2. FRAUD PROFILES AGGREGATION")
        print("="*50)
        fraud_df = self.spark.read.parquet("hdfs://localhost:9000/user/student/assignment/batch/fraud_profiles")
        fraud_df.show()

        self.spark.stop()

if __name__ == "__main__":
    viewer = BatchResultsViewer()
    viewer.display_aggregations()