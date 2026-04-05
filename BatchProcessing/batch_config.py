# Author: Ng Zhi Xuan
import json

class BatchConfig:
    def __init__(self, config_file="config.json"):
        with open(config_file, 'r') as file:
            self.config = json.load(file)

    def get_raw_input_path(self):
        return self.config['raw_input_path']

    def get_curated_output_path(self):
        return self.config['curated_output_path']

    def get_batch_output_volume(self):
        return self.config['batch_output_volume']

    def get_batch_output_fraud(self):
        return self.config['batch_output_fraud']
