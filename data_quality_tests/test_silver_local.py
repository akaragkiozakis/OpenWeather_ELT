"""
Silver Layer Data Quality Testing with Great Expectations
Testing transformed weather and mobility data
"""

import pandas as pd
import great_expectations as ge
import numpy as np
from datetime import datetime, date
import os

def log_message(message):
    """Simple logging function"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def create_sample_silver_weather_data():
    """Create sample transformed weather data (after exploding daily arrays)"""
    sample_data = {
        'date': [date(2020, 2, 15), date(2020, 2, 16), date(2020, 2, 17)],
        'temperature_max': [15.2, 16.1, 14.8],
        'temperature_min': [8.3, 9.1, 7.2],
        'precipitation_sum': [0.0, 2.3, 0.0],
        'snowfall_sum': [0.0, 0.0, 0.0],
        'wind_speed_max': [12.5, 15.2, 10.8],
        'latitude': [37.9755, 37.9755, 37.9755],
        'longitude': [23.7348, 23.7348, 23.7348],
        'ingestion_time': ['2025-10-23 10:00:00', '2025-10-23 10:00:00', '2025-10-23 10:00:00']
    }
    return pd.DataFrame(sample_data)

def create_sample_silver_mobility_data():
    """Create sample cleaned mobility data"""
    sample_data = {
        'date': [date(2020, 2, 15), date(2020, 2, 16), date(2020, 2, 17)],
        'sub_region_1': ['Attica', 'Central Macedonia', 'Crete'],
        'workplaces_percent_change_from_baseline': [-5.2, -3.1, -7.8],
        'transit_stations_percent_change_from_baseline': [-12.5, -8.3, -15.2],
        'parks_percent_change_from_baseline': [25.3, 18.7, 32.1],
        'residential_percent_change_from_baseline': [2.1, 1.5, 3.2],
        'ingestion_time': ['2025-10-23 10:00:00', '2025-10-23 10:00:00', '2025-10-23 10:00:00']
    }
    return pd.DataFrame(sample_data)

def test_silver_weather_expectations(df):
    """Test silver weather data expectations"""
    log_message("🌤 Testing Silver Weather Data Expectations...")
    
    # Convert to Great Expectations DataFrame
    ge_df = ge.from_pandas(df)
    
    test_results = []
    
    # Test 1: Table should exist and have more rows than bronze (after exploding)
    result1 = ge_df.expect_table_row_count_to_be_between(min_value=1, max_value=5000000)
    test_results.append(("Row Count Check", result1["success"]))
    
    # Test 2: Required columns after transformation
    required_columns = ['date', 'temperature_max', 'temperature_min', 'precipitation_sum', 
                       'snowfall_sum', 'wind_speed_max', 'latitude', 'longitude']
    for col in required_columns:
        result = ge_df.expect_column_to_exist(column=col)
        test_results.append((f"Column '{col}' exists", result["success"]))
    
    # Test 3: Critical columns should not be null
    critical_columns = ['date', 'latitude', 'longitude']
    for col in critical_columns:
        result = ge_df.expect_column_values_to_not_be_null(column=col)
        test_results.append((f"{col} Not Null", result["success"]))
    
    # Test 4: Temperature range checks for Greece
    result4 = ge_df.expect_column_values_to_be_between(
        column='temperature_max', min_value=-50, max_value=60
    )
    test_results.append(("Temperature Max Range", result4["success"]))
    
    result5 = ge_df.expect_column_values_to_be_between(
        column='temperature_min', min_value=-50, max_value=60
    )
    test_results.append(("Temperature Min Range", result5["success"]))
    
    # Test 5: Logic check - max temperature >= min temperature
    # Create a custom check by adding a temporary column
    df_copy = df.copy()
    df_copy['temp_logic_check'] = df_copy['temperature_max'] >= df_copy['temperature_min']
    ge_df_logic = ge.from_pandas(df_copy)
    
    result6 = ge_df_logic.expect_column_values_to_be_in_set(
        column='temp_logic_check', value_set=[True]
    )
    test_results.append(("Temperature Logic Check (Max >= Min)", result6["success"]))
    
    # Test 6: Precipitation should be non-negative
    result7 = ge_df.expect_column_values_to_be_between(
        column='precipitation_sum', min_value=0, max_value=1000
    )
    test_results.append(("Precipitation Range", result7["success"]))
    
    # Test 7: Wind speed should be non-negative
    result8 = ge_df.expect_column_values_to_be_between(
        column='wind_speed_max', min_value=0, max_value=200
    )
    test_results.append(("Wind Speed Range", result8["success"]))
    
    return test_results

def test_silver_mobility_expectations(df):
    """Test silver mobility data expectations"""
    log_message("🚗 Testing Silver Mobility Data Expectations...")
    
    # Convert to Great Expectations DataFrame
    ge_df = ge.from_pandas(df)
    
    test_results = []
    
    # Test 1: Table should exist
    result1 = ge_df.expect_table_row_count_to_be_between(min_value=1, max_value=10000000)
    test_results.append(("Row Count Check", result1["success"]))
    
    # Test 2: Required columns after cleaning
    required_columns = ['date', 'sub_region_1', 'workplaces_percent_change_from_baseline',
                       'transit_stations_percent_change_from_baseline', 'parks_percent_change_from_baseline',
                       'residential_percent_change_from_baseline']
    for col in required_columns:
        result = ge_df.expect_column_to_exist(column=col)
        test_results.append((f"Column '{col}' exists", result["success"]))
    
    # Test 3: Critical columns should not be null after cleaning
    result3 = ge_df.expect_column_values_to_not_be_null(column='date')
    test_results.append(("Date Not Null", result3["success"]))
    
    result4 = ge_df.expect_column_values_to_not_be_null(column='sub_region_1')
    test_results.append(("Sub Region Not Null", result4["success"]))
    
    # Test 4: Percentage change values should be within reasonable ranges
    mobility_columns = [
        ('workplaces_percent_change_from_baseline', -100, 500),
        ('transit_stations_percent_change_from_baseline', -100, 500),
        ('parks_percent_change_from_baseline', -100, 1000),  # Parks can have higher changes
        ('residential_percent_change_from_baseline', -100, 500)
    ]
    
    for col, min_val, max_val in mobility_columns:
        result = ge_df.expect_column_values_to_be_between(
            column=col, min_value=min_val, max_value=max_val
        )
        test_results.append((f"{col} Range Check", result["success"]))
    
    # Test 5: Check that problematic columns were dropped
    dropped_columns = ['sub_region_2', 'metro_area', 'iso_3166_2_code', 'census_fips_code']
    for col in dropped_columns:
        # If column doesn't exist, that's good (it was dropped)
        column_exists = col in df.columns
        test_results.append((f"Column '{col}' was dropped", not column_exists))
    
    return test_results

def print_test_results(test_name, results):
    """Print formatted test results"""
    log_message(f"\n📊 {test_name} Results:")
    log_message("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_desc, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        log_message(f"{status} | {test_desc}")
        if success:
            passed += 1
    
    log_message("=" * 60)
    log_message(f"📈 Summary: {passed}/{total} tests passed ({(passed/total)*100:.1f}%)")
    
    return passed == total

def main():
    """Main function to run all silver layer data quality tests"""
    log_message("🚀 Starting Silver Layer Data Quality Tests...")
    
    # Create sample silver data
    log_message("📝 Creating sample transformed data...")
    silver_weather_df = create_sample_silver_weather_data()
    silver_mobility_df = create_sample_silver_mobility_data()
    
    log_message(f"🌤 Weather data shape: {silver_weather_df.shape}")
    log_message(f"🚗 Mobility data shape: {silver_mobility_df.shape}")
    
    # Run silver weather tests
    weather_results = test_silver_weather_expectations(silver_weather_df)
    weather_passed = print_test_results("Silver Weather Data", weather_results)
    
    print("\n")
    
    # Run silver mobility tests  
    mobility_results = test_silver_mobility_expectations(silver_mobility_df)
    mobility_passed = print_test_results("Silver Mobility Data", mobility_results)
    
    # Overall summary
    log_message("\n🎯 Silver Layer Test Summary:")
    if weather_passed and mobility_passed:
        log_message("✅ ALL SILVER TESTS PASSED! Transformation quality checks successful.")
        return True
    else:
        log_message("❌ SOME SILVER TESTS FAILED! Please review transformation logic.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)