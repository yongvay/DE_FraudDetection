# Author: Tam Wan Jin
import json
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
from auradb import *
from neo4j import GraphDatabase

class Neo4jQuery:
    first_schema = StructType([
        StructField("IP", StringType(), True),
        StructField("Num_Accounts", IntegerType(), True),
        StructField("Account_List", StringType(), True)
    ])

    second_schema = StructType([
        StructField("Suspicious_Receiver", StringType(), True),
        StructField("Receive_Count", IntegerType(), True),
        StructField("Total_Amount_Received", DoubleType(), True),
        StructField("Sender_List", ArrayType(StringType()), True)
    ])
    
    def query_potential_fraud(tx):
        query = """
        MATCH (ip:IP)<-[:USES_IP]-(a:SenderAccount)
        RETURN ip.address AS IP, 
               count(a) AS Num_Accounts,
               collect(a.id) AS Account_List
        ORDER BY Num_Accounts DESC
        """
        result = tx.run(query)
        return [record.data() for record in result]



    def query_fraud_senders(tx):
        query = """
        MATCH (s:SenderAccount)-[t:TRANSFER]->(r:ReceiverAccount:SuspiciousReceiver)
        RETURN
            r.id AS Suspicious_Receiver,
            count(t) AS Receive_Count,
            sum(toFloat(t.amount)) AS Total_Amount_Received,
            collect(DISTINCT s.id) AS  Sender_List
        ORDER BY Total_Amount_Received DESC
        """
        result = tx.run(query)
        return [record.data() for record in result]


