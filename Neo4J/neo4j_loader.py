# Author: Tam Wan Jin
from neo4j import GraphDatabase
import json
import subprocess
from auradb import *

URI = NEO4J_URI
USER = NEO4J_USERNAME
PASSWORD = NEO4J_PASSWORD

class Neo4jLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def clear_database(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def close(self):
        self.driver.close()

    def create_indexes(self):
        with self.driver.session() as session:
            session.run("""
                CREATE INDEX sender_id IF NOT EXISTS
                FOR (s:SenderAccount) ON (s.id)
            """)
            session.run("""
                CREATE INDEX receiver_id IF NOT EXISTS
                FOR (r:ReceiverAccount) ON (r.id)
            """)
            session.run("""
                CREATE INDEX ip_address IF NOT EXISTS
                FOR (ip:IP) ON (ip.address)
            """)
            
    def insert_transactions(self, data):
        with self.driver.session() as session:
            session.execute_write(self._insert_transactions_tx, data)

    @staticmethod
    def _insert_transactions_tx(tx, data):
        tx.run("""
            UNWIND $data AS r
            MERGE (s:SenderAccount {id: r.sender_account})
            MERGE (r_acc:ReceiverAccount {id: r.receiver_account})
            MERGE (s)-[t:TRANSFER {id: r.transaction_id}]->(r_acc)
            SET t.amount = toFloat(r.amount),
                t.timestamp = r.timestamp,
                t.fraud = r.is_fraud
        """, data=data)
        
    def insert_potential_fraud(self, data):
        with self.driver.session() as session:
            session.execute_write(self._insert_potential_fraud_tx, data)

    @staticmethod
    def _insert_potential_fraud_tx(tx, data):
        tx.run("""
            UNWIND $data AS r
            MERGE (s:SenderAccount {id: r.sender_account})
            SET s:PotentialFraud
        """, data=data)

    def insert_suspicious_receivers(self, data):
        with self.driver.session() as session:
            session.execute_write(self._insert_suspicious_receivers_tx, data)

    @staticmethod
    def _insert_suspicious_receivers_tx(tx, data):
        tx.run("""
            UNWIND $data AS r
            MERGE (r_acc:ReceiverAccount {id: r.receiver_account})
            SET r_acc:SuspiciousReceiver,
                r_acc.receive_count = toInteger(r.receive_count),
                r_acc.total_amount = toFloat(r.total_amount_received)
        """, data=data)

    def insert_ip_takeover(self, data):
        with self.driver.session() as session:
            session.execute_write(self._insert_ip_takeover_tx, data)

    @staticmethod
    def _insert_ip_takeover_tx(tx, data):
        tx.run("""
            UNWIND $data AS r
            MERGE (ip:IP {address: r.ip_address})
            SET ip:HighRiskIP

            WITH ip, r.accounts AS acc_list
            UNWIND acc_list AS acc

            MERGE (s:SenderAccount {id: acc})-[:USES_IP]->(ip)
        """, data=data)

def read_hdfs(path):
    cmd = f"hdfs dfs -cat {path}/*"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error reading HDFS path: {path}")
        print(result.stderr)
        return []

    return [
        json.loads(line)
        for line in result.stdout.split("\n")
        if line.strip()
    ]

if __name__ == "__main__":
    loader = Neo4jLoader(URI, USER, PASSWORD)

    print("Clearing Neo4j...")
    loader.clear_database()

    print("Creating indexes...")
    loader.create_indexes()

    print("Loading transactions...")
    tx_data = read_hdfs("/user/hadoop/data/potential_offenders")
    loader.insert_transactions(tx_data)
    loader.insert_potential_fraud(tx_data)

    print("Loading suspicious receivers...")
    susp_data = read_hdfs("/user/hadoop/data/suspicious_receivers")
    loader.insert_suspicious_receivers(susp_data)

    print("Loading IP takeover...")
    ip_data = read_hdfs("/user/hadoop/data/ip_takeover")
    for r in ip_data: # Ensure Accounts is List
        if "accounts" not in r or not isinstance(r["accounts"], list):
            r["accounts"] = []
    loader.insert_ip_takeover(ip_data)

    loader.close()
    print("All data SUCCESSFULLY loaded into Neo4j")




