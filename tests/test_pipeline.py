import pytest
from pyspark.sql import SparkSession
from src.pipeline import (
    classify_channel_type,
    evaluate_channel_health,
    calculate_device_hour_health,
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("ModemTest").master("local[1]").getOrCreate()

def test_channel_type(spark):
    # Test frequencies: 999999999 (SC-QAM), 1000000000 (OFDM), None (UNKNOWN)
    data = [(999999999,), (1000000000,), (None,)]
    df = spark.createDataFrame(data, ["data_ds_freq"])
    result = classify_channel_type(df)
    types = [row.channel_type for row in result.collect()]
    assert types == ["SC-QAM", "OFDM", "UNKNOWN"]