import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import configparser
from util.commonUtil import commonUtil
from pyspark.sql.functions import date_format, col

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config = configparser.ConfigParser()
config.read("../cfg/streaming.cfg")
kafka_params = dict(config.items("kafka"))

config.read("script.cfg")
raw_zone_path = config.get("raw_zone", "hdfs_path")
checkpoint_path = config.get("checkpoint", "checkpoint_path")

value_schema = commonUtil.load_schema_from_json("../cfg/schema.json")

# Create Spark session
spark = SparkSession.builder.appName("StreamingAndHDFSWrite").getOrCreate()

try:
    # Read from Kafka
    logger.info("Reading from Kafka...")
    kafka_stream_df = spark.readStream.format("kafka").options(**kafka_params).load()

    # Now, create the StructType for the nested XML schema
    nested_xml_schema = commonUtil.xmlschemaGenerator(value_schema)

    # Deserialize the Kafka message value using the dynamically loaded schema
    parsed_df = kafka_stream_df.select(
        from_json(col("value").cast("string"), nested_xml_schema).alias("data")
    ).select("data.*")

    # Define a watermark column to handle late arriving data
    watermarked_df = parsed_df.withWatermark("loaddate", "10 minutes")

    # Define a stateful operation to handle de-duplication and apply validations
    def update_state(key, iterator, state):
        unique_keys = set(state.get("keys", []))
        output_rows = []

        for row in iterator:
            # Validate and format the data
            validated_and_formatted_row = commonUtil.validate_and_format(row)

            if validated_and_formatted_row is not None:
                # De-duplication
                if validated_and_formatted_row["ID"] not in unique_keys:
                    unique_keys.add(validated_and_formatted_row["ID"])
                    output_rows.append(validated_and_formatted_row)

        state["keys"] = list(unique_keys)
        return output_rows, state

    # Apply the stateful operation
    stateful_df = watermarked_df.groupByKey("ID").flatMapGroupsWithState(
        update_state,
        outputMode="append",
        stateSchema=StructType([
            {"name": "keys", "type": "array", "elementType": "integer", "containsNull": False}
        ])
    )

    # Add new columns for date and hour extracted from the 'loaddate'
    partitioned_df = stateful_df.withColumn("load_date", date_format("loaddate", "yyyy-MM-dd"))
    partitioned_df = partitioned_df.withColumn("load_hour", date_format("loaddate", "HH"))


    # Write to HDFS Parquet - RAW Zone
    logger.info("Writing to HDFS Parquet - RAW Zone...")
    query = (
        partitioned_df.writeStream.format("parquet")
        .option("path", raw_zone_path)
        .outputMode("append")
        .trigger(processingTime="30 seconds")  # Adjust as needed
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("load_date","load_hour")
        .start()
    )

     # Write to a different Kafka topic
    logger.info("Writing to a different Kafka topic...")
    kafka_publish_params = dict(config.items("kafka_publish"))
    kafka_topic = kafka_publish_params["topic"]

    kafka_stream_df.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_publish_params["bootstrap_servers"]) \
        .option("topic", kafka_topic) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/kafka_checkpoint")\
        .start()


    # Await termination
    logger.info("Awaiting termination...")
    query.awaitTermination()

except Exception as e:
    # Log the error for analysis
    logger.error(f"Error: {str(e)}")

finally:
    # Ensure any necessary cleanup or finalization
    logger.info("Stopping the Spark session...")
    spark.stop()
