from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from datetime import datetime, timedelta
import logging
import configparser


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
config = configparser.ConfigParser()

config.read("script.cfg")


# Create a Spark session
spark = SparkSession.builder.appName("UpsertExample").getOrCreate()

try:
    # Define the paths for RAW and Processed Zones
    raw_zone_path = config.get("raw_zone", "hdfs_path")
    processed_zone_path = config.get("processed_zone", "hdfs_path")
    num_process_hour = config.get("num_process_hour", "hdfs_path")

    # Calculate the desired partition date and hour (1 hour ago)
    current_date = datetime.now().strftime("%Y-%m-%d")
    desired_hour = (datetime.now() - timedelta(hours=num_process_hour)).strftime("%H")

    # Read data from RAW Zone with filters on date and hour
    raw_data_df = (
        broadcast(spark.read.parquet(raw_zone_path))
        .filter(col("date") == current_date)
        .filter(col("hour") == desired_hour)
    )

    # Perform upsert logic (example: update 'processed_data_df' based on the key column 'ID')
    processed_data_df = spark.read.parquet(processed_zone_path)
    upserted_data_df = raw_data_df.join(
        processed_data_df, raw_data_df["ID"] == processed_data_df["ID"], "left"
    ).select(
        raw_data_df["ID"],
        col("raw_column1").alias("processed_column1"),  # Example: Rename columns as needed
        col("raw_column2").alias("processed_column2")
        # Add other columns as needed
    ).fillna(raw_data_df)

    unmatched = processed_data_df.join(upserted_data_df,processed_data_df["ID"] == upserted_data_df["ID"],"leftanti" )
    union_unmacted_upserted  = unmatched.union(upserted_data_df);
    # Write the upserted data to the Processed Zone, append to partitions based on loaddate
    union_unmacted_upserted.coalesce(10).write.mode("overwrite").partitionBy("loaddate").parquet(processed_zone_path)

    # Log success
    logger.info("Upsert operation completed successfully.")

except Exception as e:
    # Log the error for analysis
    logger.error(f"Error: {str(e)}")

finally:
    # Stop the Spark session
    spark.stop()
