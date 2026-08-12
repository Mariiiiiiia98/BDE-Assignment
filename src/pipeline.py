from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def classify_channel_type(df: DataFrame) -> DataFrame:
    """Classifies channels based on frequency"""
    return df.withColumn(
        "channel_type",
        F.when(F.col("data_ds_freq").isNull(), "UNKNOWN")
         .when(F.col("data_ds_freq") < 1000000000, "SC-QAM")
         .otherwise("OFDM")
    )

def evaluate_channel_health(df: DataFrame) -> DataFrame:
    """Evaluates channel health status for SC-QAM channels"""
    is_unhealthy = (
        (F.col("data_ds_snr") < 31.0) |
        (F.col("data_ds_rxp") < -8.0) |
        (F.col("data_ds_rxp") > 15.0) |
        (F.col("data_ds_corr_rate") >= 0.25)
    )

    is_missing = (
        F.col("data_ds_snr").isNull() |
        F.col("data_ds_rxp").isNull() |
        F.col("data_ds_corr_rate").isNull()
    )

    return df.withColumn(
        "channel_status",
        F.when(F.col("channel_type") != "SC-QAM", "UNKNOWN")
         .when(is_missing, "UNKNOWN")
         .when(is_unhealthy, "UNHEALTHY")
         .otherwise("HEALTHY")
    )
def calculate_device_hour_health(df: DataFrame) -> DataFrame:
    """Aggregates channel health per device per hour"""
    df_with_hour = df.withColumn(
        "measurement_hour",
        F.date_trunc("hour", F.to_timestamp("start_time_cet"))
    )

    grouped = df_with_hour.groupBy("data_mac", "measurement_hour").agg(
        F.sum(F.when(F.col("channel_status") == "UNHEALTHY", 1).otherwise(0)).alias("unhealthy_count"),
        F.sum(F.when(F.col("channel_status") == "HEALTHY", 1).otherwise(0)).alias("healthy_count")
    )

    return grouped.withColumn(
        "device_hour_status",
        F.when(F.col("unhealthy_count") > 0, "UNHEALTHY")
         .when((F.col("healthy_count") > 0) & (F.col("unhealthy_count") == 0), "HEALTHY")
         .otherwise("UNKNOWN")
    ).select("data_mac", "measurement_hour", "device_hour_status")

def calculate_device_health(df_device_hour: DataFrame) -> DataFrame:
    """Aggregates hourly health status into overall device health"""
    grouped = df_device_hour.groupBy("data_mac").agg(
        F.sum(F.when(F.col("device_hour_status") == "UNHEALTHY", 1).otherwise(0)).alias("unhealthy_hours"),
        F.sum(F.when(F.col("device_hour_status") == "HEALTHY", 1).otherwise(0)).alias("healthy_hours"),
        F.sum(F.when(F.col("device_hour_status") == "UNKNOWN", 1).otherwise(0)).alias("unknown_hours"),
        F.count("device_hour_status").alias("total_hours")
    )

    return grouped.withColumn(
        "device_status",
        F.when(F.col("unhealthy_hours") > 0, "UNHEALTHY")
         .when(F.col("healthy_hours") == F.col("total_hours"), "HEALTHY")
         .otherwise("UNKNOWN")
    ).select("data_mac", "device_status")