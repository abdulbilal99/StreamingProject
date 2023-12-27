Usage
Spark Streaming
To run the Spark Streaming job, execute the following command:

bash ./script/streaming.sh

This script sets up the necessary configurations and submits the Spark Streaming job using streaming.py.

Spark Batch Processing
To run the Spark Batch Processing job, execute the following command:

bash ./script/batch.sh

This script sets up the necessary configurations and submits the Spark Batch Processing job using batchProcess.py.

Configuration
Adjust configuration parameters in batch.cfg and streaming.cfg to match your specific requirements.
Ensure that the JSON schema in schema.json aligns with your data structure.
Dependencies
Ensure that you have Apache Spark installed and configured on your cluster. Additionally, verify that the necessary Python dependencies are installed.
