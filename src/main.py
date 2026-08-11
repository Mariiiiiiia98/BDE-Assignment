import argparse
import os
from pyspark.sql import SparkSession
from src.pipeline import (
    classify_channel_type,
    evaluate_channel_health,
    calculate_device_hour_health,
    calculate_device_health,
)


def main(input_path: str, output_dir: str):
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CableModemHealthCheck") \
        .getOrCreate()

    # Read input telemetry data (supports CSV or Parquet)
    if input_path.endswith(".csv"):
        df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    else:
        df_raw = spark.read.parquet(input_path)

    # 1. Channel Health Evaluation
    df_classified = classify_channel_type(df_raw)
    df_channel_health = evaluate_channel_health(df_classified)

    # 2. Device Hour Health Aggregation
    df_device_hour_health = calculate_device_hour_health(df_channel_health)

    # 3. Overall Device Health Aggregation
    df_device_health = calculate_device_health(df_device_hour_health)

    # Write each dataset to its required subfolder
    df_channel_health.write.mode("overwrite").parquet(os.path.join(output_dir, "channel_health"))
    df_device_hour_health.write.mode("overwrite").parquet(os.path.join(output_dir, "device_hour_health"))
    df_device_health.write.mode("overwrite").parquet(os.path.join(output_dir, "device_health"))

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cable Modem Health Check Pipeline")
    parser.add_argument("--input-path", required=True, help="Path to input dataset (CSV or Parquet)")
    parser.add_argument("--output-dir", required=True, help="Root directory for output datasets")
    args = parser.parse_args()

    main(args.input_path, args.output_dir)
