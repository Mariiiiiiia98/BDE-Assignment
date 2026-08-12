# BDE-Assignment

A PySpark data processing pipeline designed to ingest raw broadband cable modem telemetry data and evaluate network health across three levels: channel level, device-hour aggregate level, and overall device fleet level.

## Project Structure

```text
├── data/
│   ├── downstream_csq_spec.csv  # Business logic specification
│   └── ds_processed.csv         # Raw input telemetry dataset
├── output/
│   ├── channel_health/          # Output 1: Evaluated channel-level metrics
│   ├── device_hour_health/      # Output 2: Hourly aggregated device health
│   └── device_health/           # Output 3: Overall fleet device health
├── src/
│   ├── main.py                  # Main PySpark processing pipeline entry point
│   └── pipeline.py              # Core transformation and health logic
├── tests/
│   └── test_pipeline.py         # Unit tests using pytest
├── .gitignore
├── README.md                    # Project documentation
└── requirements.txt             # Python dependencies

1. Prerequisites:
   pip install -r requirements.txt

2. How to run the pipeline:
   python -m src.main --input-path data/ds_processed.csv --output-dir output/

3. Generated outputs inside the output/ directory:
   - output/channel_health/
   - output/device_hour_health/
   - output/device_health/

4. How to run tests:
   pytest -q