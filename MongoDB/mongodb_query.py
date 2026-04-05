# Author: Chan Xing Szen
class MongoDBQuerier:
    
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def get_comprehensive_risk_analysis(self, base_col, receivers_col, iptakeover_col):
        collection = self.db_connector.get_collection(base_col)
        
        pipeline = [
            {
                "$addFields": {
                    "num_velocity": {"$toDouble": "$velocity_score"},
                    "num_geo": {"$toDouble": "$geo_anomaly_score"},
                    "num_amount": {"$toDouble": "$amount"}
                }
            },
            {
                "$lookup": {
                    "from": receivers_col,
                    "localField": "receiver_account",
                    "foreignField": "receiver_account",
                    "as": "receiver_data"
                }
            },
            {
                "$lookup": {
                    "from": iptakeover_col,
                    "localField": "ip_address",
                    "foreignField": "ip_address",
                    "as": "ip_data"
                }
            },
            {
                "$addFields": {
                    "receiver_flag": {"$gt": [{"$size": "$receiver_data"}, 0]},
                    "ip_flag": {"$gt": [{"$size": "$ip_data"}, 0]},
                    "base_risk": {"$add": ["$num_velocity", {"$multiply": ["$num_geo", 20]}]}
                }
            },
            {
                "$addFields": {
                    "composite_risk_score": {
                        "$add": [
                            "$base_risk",
                            {"$cond": [{"$eq": ["$receiver_flag", True]}, 50, 0]},
                            {"$cond": [{"$eq": ["$ip_flag", True]}, 50, 0]}
                        ]
                    },
                    "threat_level": {
                        "$switch": {
                            "branches": [
                                {"case": {"$or": ["$receiver_flag", "$ip_flag"]}, "then": "CRITICAL (MULTI-VECTOR)"},
                                {"case": {"$gte": ["$base_risk", 35]}, "then": "HIGH (ANOMALY)"}
                            ],
                            "default": "ELEVATED (VELOCITY)"
                        }
                    }
                }
            },
            {
                "$sort": {"composite_risk_score": -1}
            },
            {
                "$limit": 15
            },
            {
                "$project": {
                    "_id": 0,
                    "sender_account": 1,
                    "receiver_account": 1,
                    "ip_address": 1,
                    "amount": 1,
                    "threat_level": 1,
                    "composite_risk_score": {"$round": ["$composite_risk_score", 2]},
                    "receiver_flag": 1,
                    "ip_flag": 1
                }
            }
        ]
        
        return list(collection.aggregate(pipeline))

    def get_dynamic_risk_profiles(self, collection_name, min_volume=1000):
        collection = self.db_connector.get_collection(collection_name)
        
        pipeline = [
            {
                "$match": {
                    "total_count": {"$gte": min_volume}
                }
            },
            {
                "$addFields": {
                    "risk_category": {
                        "$switch": {
                            "branches": [
                                {"case": {"$gte": ["$fraud_percentage", 4.0]}, "then": "CRITICAL"},
                                {"case": {"$gte": ["$fraud_percentage", 3.6]}, "then": "HIGH"}
                            ],
                            "default": "MODERATE"
                        }
                    }
                }
            },
            {
                "$match": {
                    "risk_category": {"$in": ["CRITICAL", "HIGH"]}
                }
            },
            {
                "$sort": {"fraud_percentage": -1}
            },
            {
                "$project": {
                    "_id": 0,
                    "transaction_type": 1,
                    "total_count": 1,
                    "fraud_percentage": 1,
                    "risk_category": 1
                }
            }
        ]
        
        return list(collection.aggregate(pipeline))