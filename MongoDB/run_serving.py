# Author: Chan Xing Szen
import json
import os
from pyspark.sql import SparkSession

from mongodb_connector import MongoDBConnector
from mongodb_ingestor import MongoDBIngestor
from mongodb_query import MongoDBQuerier

class ServingLayerRunner:
    def __init__(self, config_file_path):
        with open(config_file_path, 'r') as file:
            self.config = json.load(file)
            
        self.spark = SparkSession.builder \
            .appName("MongoDB_Serving_Layer") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0")\
            .getOrCreate()

        self.connector = MongoDBConnector(self.config['mongodb_uri'], self.config['mongodb_db'])
        self.ingestor = MongoDBIngestor(self.connector, self.spark)
        self.querier = MongoDBQuerier(self.connector)

        self.offenders_col = self.config['mongodb_offenders_collection']
        self.profiles_col = self.config['mongodb_profiles_collection']
        self.receivers_col = self.config['mongodb_receivers_collection']
        self.iptakeover_col = self.config['mongodb_iptakeover_collection']

    def execute_ingestion(self):
        print("Ingesting curated data into MongoDB...")
        self.ingestor.ingest_realtime_data(self.config['potential_offenders_hdfs_path'], self.offenders_col)
        self.ingestor.ingest_batch_data(self.config['batch_output_fraud'], self.profiles_col)
        self.ingestor.ingest_realtime_data(self.config['suspicious_receivers_hdfs_path'], self.receivers_col)
        self.ingestor.ingest_realtime_data(self.config['ip_takeover_hdfs_path'], self.iptakeover_col)

    def execute_queries_and_display(self):
        print("Running complex analytical aggregations...")

        cross_reference_data = self.querier.get_comprehensive_risk_analysis(
            self.offenders_col, 
            self.receivers_col, 
            self.iptakeover_col
        )

        risk_profiles_data = self.querier.get_dynamic_risk_profiles(self.profiles_col, 1000)

        print("\n--- Query 1: Cross-Referenced Real-Time Threats ---")
        if cross_reference_data:
            with open("output_cross_referenced.json", "w") as f:
                for record in cross_reference_data:
                    f.write(json.dumps(record) + "\n")
            
            local_cross_path = f"file://{os.path.abspath('output_cross_referenced.json')}"
            cross_df = self.spark.read.json(local_cross_path)
            cross_df.show(truncate=False) 
        else:
            print("No cross-referenced threats found matching the criteria.")

        print("\n--- Query 2: Dynamic Batch Risk Profiling ---")
        if risk_profiles_data:
            with open("output_profiles.json", "w") as f:
                for record in risk_profiles_data:
                    f.write(json.dumps(record) + "\n")

            local_profiles_path = f"file://{os.path.abspath('output_profiles.json')}"
            profiles_df = self.spark.read.json(local_profiles_path)
            profiles_df.show(truncate=False) 
        else:
            print("No risk profiles found matching the criteria in MongoDB.")

    def shutdown(self):
        self.connector.close()
        self.spark.stop()

if __name__ == "__main__":
    runner = ServingLayerRunner('BatchProcessing/config.json')
    
    try:
        runner.execute_ingestion()
        runner.execute_queries_and_display()
    finally:
        runner.shutdown()