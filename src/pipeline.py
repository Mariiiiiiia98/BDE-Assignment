from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def classify_channel_type(df: DataFrame) -> DataFrame:
    """Classifies channels based on frequency."""
    return df.withColumn(
        "channel_type",
        F.when(F.col("data_ds_freq").isNull(), "UNKNOWN")
         .when(F.col("data_ds_freq") < 1000000000, "SC-QAM")
         .otherwise("OFDM")
    )
