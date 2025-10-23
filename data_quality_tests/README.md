# Data Quality Tests

This folder contains Great Expectations data quality tests for the OpenWeather ELT project.

## Files

### Test Scripts
- `test_bronze_simple.py` - Simple bronze layer data quality tests (no emojis, Windows-compatible)
- `test_bronze_local.py` - Full bronze layer tests with detailed output
- `test_silver_local.py` - Silver layer transformation quality tests
- `run_all_tests.py` - Master script to run all tests

### How to Run Tests

#### Individual Tests
```bash
# Bronze layer tests (simple version)
python test_bronze_simple.py

# Bronze layer tests (full version)
python test_bronze_local.py

# Silver layer tests
python test_silver_local.py
```

#### All Tests
```bash
# Run all tests together
python run_all_tests.py
```

## What the Tests Check

### Bronze Layer Tests
- Table existence and row counts
- Required columns present
- Geographic coordinates within valid ranges (-90 to 90 for latitude, -180 to 180 for longitude)
- No null values in critical fields
- Data type validations

### Silver Layer Tests
- Successful data transformation (exploded daily arrays)
- Temperature values within realistic ranges for Greece (-50°C to 60°C)
- Logic checks (max temperature >= min temperature)
- Precipitation and wind speed non-negative values
- Mobility percentage changes within reasonable ranges
- Dropped columns validation (unwanted columns removed)

## Test Data

The tests use sample data that mimics the structure of your real weather and mobility data:

- **Weather data**: Greek coordinates (Athens, Thessaloniki, Heraklion) with realistic temperature ranges
- **Mobility data**: Greek regions (Attica, Central Macedonia, Crete) with COVID-19 mobility changes

## Integration with Your Pipeline

To integrate these tests into your Databricks scripts:

1. Add quality checks after data ingestion (Bronze layer)
2. Add quality checks after data transformation (Silver layer)
3. Stop pipeline if critical tests fail
4. Log all test results

## Dependencies

- `great-expectations>=0.15.50`
- `pandas>=2.0.0`
- `numpy` (for data manipulation)

## Output

Tests provide:
- ✅ PASS/❌ FAIL status for each expectation
- Summary statistics (X/Y tests passed)
- Overall success/failure status
- Detailed logging with timestamps

## Troubleshooting

If you see Unicode errors on Windows, use `test_bronze_simple.py` instead of the emoji versions.