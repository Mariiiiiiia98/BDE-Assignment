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

def test_device_hour_health(spark):
    # Test device-hour aggregation rules
    data = [
        ("mac_001", "2026-08-11 10:00:00", "HEALTHY"),
        ("mac_001", "2026-08-11 10:00:00", "HEALTHY"), # All healthy -> HEALTHY
        ("mac_002", "2026-08-11 10:00:00", "HEALTHY"),
        ("mac_002", "2026-08-11 10:00:00", "UNHEALTHY"), # One unhealthy -> UNHEALTHY
    ]
    df = spark.createDataFrame(data, ["data_mac", "start_time_cet", "channel_status"])
    result = calculate_device_hour_health(df)
    status_dict = {row.data_mac: row.device_hour_status for row in result.collect()}
    
    assert status_dict["mac_001"] == "HEALTHY"
    assert status_dict["mac_002"] == "UNHEALTHY"

def test_device_health(spark):
    #Device overall health
    from src.pipeline import aggregate_device_health

    # Mock hourly health data
    data = [
        # Device 1: Has one UNHEALTHY hour -> overall UNHEALTHY
        ("MAC_01", "2026-01-20 00:00:00", "HEALTHY"),
        ("MAC_01", "2026-01-20 01:00:00", "UNHEALTHY"),
        
        # Device 2: All hours HEALTHY -> overall HEALTHY
        ("MAC_02", "2026-01-20 00:00:00", "HEALTHY"),
        ("MAC_02", "2026-01-20 01:00:00", "HEALTHY"),
        
        # Device 3: All hours UNKNOWN -> overall UNKNOWN
        ("MAC_03", "2026-01-20 00:00:00", "UNKNOWN"),
        ("MAC_03", "2026-01-20 01:00:00", "UNKNOWN"),
    ]

    schema = ["data_mac", "measurement_hour", "device_hour_status"]
    df_input = spark.createDataFrame(data, schema)

    df_result = aggregate_device_health(df_input)
    results = {row["data_mac"]: row["device_status"] for row in df_result.collect()}

    assert results["MAC_01"] == "UNHEALTHY"
    assert results["MAC_02"] == "HEALTHY"
    assert results["MAC_03"] == "UNKNOWN"