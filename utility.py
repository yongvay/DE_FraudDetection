import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Set up a basic logger if one isn't configured at the project level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Utilities:
    """Utility class to manage Kafka topics across the project."""
    
    def __init__(self, bootstrap_servers: list | str, client_id: str = 'wsl-kafka-admin'):
        """
        Initializes the manager with the cluster connection details.
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id

    def create_topic_if_missing(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        """Checks if a topic exists on the broker, and creates it if it doesn't."""
        
        # 1. Initialize the Admin Client
        admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id
        )
        
        try:
            # 2. Get a list of all current topics on the broker
            existing_topics = admin_client.list_topics()
            
            # 3. Check and create
            if topic_name not in existing_topics:
                logger.info(f"Topic '{topic_name}' not found. Creating it now...")
                
                # Define the configuration for the new topic
                topic_config = NewTopic(
                    name=topic_name, 
                    num_partitions=num_partitions, 
                    replication_factor=replication_factor
                )
                
                # Ask the broker to create it
                admin_client.create_topics(new_topics=[topic_config], validate_only=False)
                logger.info(f"Successfully created topic '{topic_name}' with {num_partitions} partitions.")
            else:
                logger.info(f"Topic '{topic_name}' already exists. Skipping creation.")
                
        except TopicAlreadyExistsError:
            # Catches the race condition where another script creates it simultaneously
            logger.info(f"Topic '{topic_name}' already exists (caught via exception).")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
            raise # Optionally re-raise the error so the calling script knows it failed
        finally:
            # Always close the admin client to free up resources
            admin_client.close()
