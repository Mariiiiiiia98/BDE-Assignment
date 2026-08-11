import pytest
from pyspark.sql import SparkSession
from src.pipeline import (
    classify_channel_type,
    evaluate_channel_health,
    calculate_device_hour_health,
    calculate_device_health,
)

@pytest.fixture(scope="session")
def spark():
    # Starts a simple local Spark engine for your tests
    return SparkSession.builder \
        .appName("CableModemTest") \
        .master("local[2]") \
        .getOrCreate()