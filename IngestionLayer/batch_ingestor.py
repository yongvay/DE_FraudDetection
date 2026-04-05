# Author: Ng Yong Vay
import subprocess
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchIngestor:
    def __init__(self, hdfs_base_dir: str):
        self.hdfs_base_dir = hdfs_base_dir
        self._ensure_hdfs_directory(self.hdfs_base_dir)

    def _run_cmd(self, cmd: list) -> bool:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {' '.join(cmd)}\nError: {e.stderr.strip()}")
            return False

    def _ensure_hdfs_directory(self, hdfs_path: str):
        logger.info(f"Ensuring HDFS directory exists: {hdfs_path}")
        
        cmd = ["hdfs", "dfs", "-mkdir", "-p", hdfs_path]
        if self._run_cmd(cmd):
            logger.info("HDFS directory is ready.")

    def ingest_file(self, local_file_path: str, hdfs_target_name: str = None):
        if not os.path.exists(local_file_path):
            logger.error(f"Local file not found: {local_file_path}")
            return

        file_name = hdfs_target_name if hdfs_target_name else os.path.basename(local_file_path)
        target_path = f"{self.hdfs_base_dir}/{file_name}"
        
        logger.info(f"Starting batch ingestion: {local_file_path} -> {target_path}")

        cmd = ["hdfs", "dfs", "-put", "-f", local_file_path, target_path]
        
        if self._run_cmd(cmd):
            logger.info(f"Successfully ingested {file_name} to HDFS.")
            self._verify_upload(target_path)

    def _verify_upload(self, hdfs_path: str):
        cmd = ["hdfs", "dfs", "-ls", hdfs_path]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output_parts = result.stdout.strip().split()
            if len(output_parts) >= 5:
                size_bytes = output_parts[4]
                logger.info(f"Verification passed: {hdfs_path} exists in HDFS. Size: {size_bytes} bytes.")
        except subprocess.CalledProcessError:
            logger.warning(f"Failed to verify upload for {hdfs_path}")

if __name__ == "__main__":
    logger.info("=== STARTING BATCH INGESTION PIPELINE ===")
    
    LOCAL_CSV_FILE = "../data/sample_100000_sorted.csv"
    logger.info(f"Resolved absolute local path: {LOCAL_CSV_FILE}")
    
    HDFS_RAW_ZONE = "assignment/raw/financial_transactions"
    logger.info(f"Target HDFS Zone: {HDFS_RAW_ZONE}")

    ingestor = BatchIngestor(hdfs_base_dir=HDFS_RAW_ZONE)
    ingestor.ingest_file(local_file_path=LOCAL_CSV_FILE)
    
    logger.info("=== BATCH INGESTION PIPELINE COMPLETED ===")