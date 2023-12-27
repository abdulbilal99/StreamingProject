#assuming the emr cluster
# R6g.xlarge  - 1  primary
# R6g.xlarge - 15 core

spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --driver-memory 8g \
  --executor-memory 25g \
  --executor-cores 3 \
  --num-executors 12 \
  s3://YOUR_S3_BUCKET/path/to/src/batchprocess.py


