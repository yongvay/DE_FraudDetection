===========================================================
BMDS2013 Data Engineering Assignment – readme.txt
===========================================================


Team Reference: S3G4-3
Submission Date: 3/4/2026


-----------------------------------------------------------
1. Project Title: 
-----------------------------------------------------------
Real-Time Fraud Transaction Detection


-----------------------------------------------------------
2. Project Folder Structure: 
-----------------------------------------------------------
fraud_detection_project/
│
├── requirements.txt
├── readme.txt
├── main.py / run_pipiline.ipynb  
├── 
│
├── IngestionLayer/
│   ├── batch_ingestor.py        
│   ├── real_time_ingestor.py        
│    
├── BatchProcessing/
│   ├── batch_config.py                
│   ├── batch_processor.py
│   └── main_batch.py
│   └── show_results.py
│
├── RealTimeProcessing/
│   ├── RealTime.py               
│
├── MongoDB/
│   ├── run_serving.py        
│   ├── mongodb_connector.py       
│   ├── mongodb_ingestor.py  
|   ├── mongodb_query.py
|
|──Neo4j/       
│   ├── neo4j_loader.py       
│   ├── auradb.py
|   ├── neo4j_query.py
│                 
│
└── data/
    ├── sample_100000_sorted.csv                
    └── 5000_Basic.csv 
    └── sample_1500_sorted.csv






-----------------------------------------------------------
3. Setup Instructions:
-----------------------------------------------------------
3.1 Install dependencies:
    $ pip install -r requirements.txt


3.2 Prepare the data 
Save the “G4-3_Data” folder, which contains three datasets, to your WSL account and to HDFS. Make sure these three datasets are stored in the same directory in the WSL account and HDFS:
* sample_100000_sorted.csv
* 5000_Advanced.csv
* 5000_Basic.csv


3.3 Start the necessary services . . .
        $ start-dfs.sh
        $ start-yarn.sh
        $ zookeeper-server-start.sh
        $ $KAFKA_HOME/config/zookeeper.properties &
        $ kafka-server-start.sh $KAFKA_HOME/config/server.properties &
        $ nano ~/.profile
--And add the following lines at the end of the file.
export SPARK_HOME=/home/hduser/spark
export PATH=$PATH:$SPARK_HOME/bin
export KAFKA_HOME=/home/hduser/kafka
export PATH=$PATH:$KAFKA_HOME/bin
        $ source ~/.profile
   $ source de-venv/bin/activate


3.4 Run the demo:
1. Due to Spark's nature, all previously processed data will be ignored. Hence, always change the HDFS file path by simply adding a version name to the end. 
For example:


Your previous file path:
hdfs://localhost:9000/user/student/assignment/realtime_v1/data/suspicious_receivers


Your new file path:
hdfs://localhost:9000/user/student/assignment/realtime_v2/data/suspicious_receivers


Amend the file path for the Real Time processing, MongoDB and Neo4J Sections. 


2. Run the first cell in the Jupyter Notebook to make the file path discoverable by the script. 


3. Follow the sequence of the notebook cell and run each of them step-by-step


4. Run the following command to check any data changes in HDFS, and ensure that the highlighted part follows the version you changed in the HDFS file path in the previous section. 


        Check any new folders created
$ hdfs dfs -ls /user/student/assignment/realtime_v1/


Check real-time processed data
$ hdfs dfs -ls /user/student/assignment/realtime_v1/data


Get the processed transaction in a certain topic (Change the folder name and copy the file path from your terminal)
$ hdfs dfs -cat /user/student/assignment/realtime_v1/data/suspicious_receiver/part-00199-7107f4ff-e683-4054-a6c6-f045314c91bb-c000.json


5. Run the following notebook cells after the data exists in the HDFS






3.5 Stop the services:
        $ kafka-server-stop.sh
        $ zookeeper-server-stop.sh
        $ stop-yarn.sh
        $ stop-dfs