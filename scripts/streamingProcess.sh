#assuming the emr cluster
# C5.9xlarge - 1 primary

# C5.9xlarge - 3 core

spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --driver-memory 5g \
  --executor-memory 30g \
  --executor-cores 5 \
  --num-executors 3 \
  --conf spark. memory.offHeap.enable=true \
  --conf spark.memory.offHeap.size= 18g  \
  --conf spark.yarn.submit.waitAppCompletion=true \
  s3://YOUR_S3_BUCKET/path/to/src/streaming.py
