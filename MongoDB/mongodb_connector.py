# Author: Chan Xing Szen
from pymongo import MongoClient

class MongoDBConnector:
    
    def __init__(self, uri, db_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def close(self):
        self.client.close()