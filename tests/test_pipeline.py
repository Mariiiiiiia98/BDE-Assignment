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

def test_channel_health(spark):
    # Test rules: SNR threshold, RX Power limits, Error Rate, and OFDM/Missing handling
    data = [
        ("SC-QAM", 30.9, 5.0, 0.1),  # UNHEALTHY (SNR < 31)
        ("SC-QAM", 31.0, 5.0, 0.1),  # HEALTHY (SNR passes)
        ("SC-QAM", 35.0, 15.1, 0.1), # UNHEALTHY (RX Power > 15)
        ("SC-QAM", 35.0, 5.0, 0.25), # UNHEALTHY (Error Rate >= 0.25)
        ("OFDM", 35.0, 5.0, 0.1),    # UNKNOWN (OFDM channel)
    ]
    df = spark.createDataFrame(data, ["channel_type", "data_ds_snr", "data_ds_rxp", "data_ds_corr_rate"])
    result = evaluate_channel_health(df)
    statuses = [row.channel_status for row in result.collect()]
    assert statuses == ["UNHEALTHY", "HEALTHY", "UNHEALTHY", "UNHEALTHY", "UNKNOWN"]

