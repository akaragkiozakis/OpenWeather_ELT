"""
Simple Data Quality Testing with Great Expectations
Testing Bronze Layer Data (Local CSV/JSON files) 
"""

import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
import json
import os
from datetime import datetime

def log_message(message):
    """Simple logging function"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def create_sample_weather_data():
    """Create sample weather data for testing"""
    sample_data = {
        'latitude': [37.9755, 40.6401, 38.2466],
        'longitude': [23.7348, 22.9444, 21.7346],
        'daily': [
            {'time': ['2020-02-15', '2020-02-16'], 'temperature_2m_max': [15.2, 16.1], 'temperature_2m_min': [8.3, 9.1]},
            {'time': ['2020-02-15', '2020-02-16'], 'temperature_2m_max': [12.5, 13.2], 'temperature_2m_min': [5.1, 6.2]},
            {'time': ['2020-02-15', '2020-02-16'], 'temperature_2m_max': [14.8, 15.5], 'temperature_2m_min': [7.2, 8.0]}
        ],
        'source': ['open-meteo', 'open-meteo', 'open-meteo'],
        'file_type': ['json', 'json', 'json'],
        'ingestion_time': ['2025-10-23 10:00:00', '2025-10-23 10:00:00', '2025-10-23 10:00:00']
    }
    return pd.DataFrame(sample_data)

def create_sample_mobility_data():
    """Create sample mobility data for testing"""
    sample_data = {
        'date': ['2020-02-15', '2020-02-16', '2020-02-17'],
        'sub_region_1': ['Attica', 'Central Macedonia', 'Crete'],
        'workplaces_percent_change_from_baseline': [-5.2, -3.1, -7.8],
        'transit_stations_percent_change_from_baseline': [-12.5, -8.3, -15.2],
        'parks_percent_change_from_baseline': [25.3, 18.7, 32.1],
        'residential_percent_change_from_baseline': [2.1, 1.5, 3.2],
        'source': ['greece-mobility', 'greece-mobility', 'greece-mobility'],
        'file_type': ['csv', 'csv', 'csv'],
        'ingestion_time': ['2025-10-23 10:00:00', '2025-10-23 10:00:00', '2025-10-23 10:00:00']
    }
    return pd.DataFrame(sample_data)

def test_bronze_weather_expectations(df):
    """Test bronze weather data expectations"""
    log_message("WEATHER: Testing Bronze Weather Data Expectations...")
    
    # Convert to Great Expectations DataFrame
    ge_df = ge.from_pandas(df)
    
    test_results = []
    
    # Test 1: Table should exist (row count > 0)
    result1 = ge_df.expect_table_row_count_to_be_between(min_value=1, max_value=1000000)
    test_results.append(("Row Count Check", result1["success"]))
    
    # Test 2: Required columns should exist
    required_columns = ['latitude', 'longitude', 'daily', 'source', 'ingestion_time']
    for col in required_columns:
        result = ge_df.expect_column_to_exist(column=col)
        test_results.append((f"Column '{col}' exists", result["success"]))
    
    # Test 3: Latitude values should be valid
    result3 = ge_df.expect_column_values_to_be_between(column='latitude', min_value=-90, max_value=90)
    test_results.append(("Latitude Range Check", result3["success"]))
    
    # Test 4: Longitude values should be valid
    result4 = ge_df.expect_column_values_to_be_between(column='longitude', min_value=-180, max_value=180)
    test_results.append(("Longitude Range Check", result4["success"]))
    
    # Test 5: No null values in critical columns
    result5 = ge_df.expect_column_values_to_not_be_null(column='latitude')
    test_results.append(("Latitude Not Null", result5["success"]))
    
    return test_results

def test_bronze_mobility_expectations(df):
    """Test bronze mobility data expectations"""
    log_message("MOBILITY: Testing Bronze Mobility Data Expectations...")
    
    # Convert to Great Expectations DataFrame
    ge_df = ge.from_pandas(df)
    
    test_results = []
    
    # Test 1: Table should exist (row count > 0)
    result1 = ge_df.expect_table_row_count_to_be_between(min_value=1, max_value=10000000)
    test_results.append(("Row Count Check", result1["success"]))
    
    # Test 2: Required columns should exist
    required_columns = ['date', 'sub_region_1', 'workplaces_percent_change_from_baseline']
    for col in required_columns:
        result = ge_df.expect_column_to_exist(column=col)
        test_results.append((f"Column '{col}' exists", result["success"]))
    
    # Test 3: No null values in critical columns
    result3 = ge_df.expect_column_values_to_not_be_null(column='date')
    test_results.append(("Date Not Null", result3["success"]))
    
    result4 = ge_df.expect_column_values_to_not_be_null(column='sub_region_1')
    test_results.append(("Sub Region Not Null", result4["success"]))
    
    # Test 4: Workplace changes should be within reasonable range
    result5 = ge_df.expect_column_values_to_be_between(
        column='workplaces_percent_change_from_baseline', 
        min_value=-100, 
        max_value=500
    )
    test_results.append(("Workplace Changes Range Check", result5["success"]))
    
    return test_results

def print_test_results(test_name, results):
    """Print formatted test results"""
    log_message(f"\nRESULTS: {test_name}")
    log_message("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_desc, success in results:
        status = "PASS" if success else "FAIL"
        log_message(f"[{status}] {test_desc}")
        if success:
            passed += 1
    
    log_message("=" * 50)
    log_message(f"SUMMARY: {passed}/{total} tests passed ({(passed/total)*100:.1f}%)")
    
    return passed == total

def main():
    """Main function to run all data quality tests"""
    log_message("Starting Local Data Quality Tests...")
    
    # Create sample data
    log_message("Creating sample data...")
    weather_df = create_sample_weather_data()
    mobility_df = create_sample_mobility_data()
    
    # Run bronze weather tests
    weather_results = test_bronze_weather_expectations(weather_df)
    weather_passed = print_test_results("Bronze Weather Data", weather_results)
    
    print("\n")
    
    # Run bronze mobility tests  
    mobility_results = test_bronze_mobility_expectations(mobility_df)
    mobility_passed = print_test_results("Bronze Mobility Data", mobility_results)
    
    # Overall summary
    log_message("\nOverall Test Summary:")
    if weather_passed and mobility_passed:
        log_message("SUCCESS: ALL TESTS PASSED! Data quality checks successful.")
        return True
    else:
        log_message("FAILURE: SOME TESTS FAILED! Please review data quality issues.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)