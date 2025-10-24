# Quality Configuration for Great Expectations
# This file contains project-specific configurations for data quality tests

# Databricks paths configuration
DATABRICKS_PATHS = {
    "bronze_json": "/Volumes/weather_data/default/jason/",
    "bronze_csv": "/Volumes/weather_data/default/csv/",
    "silver_weather": "/Volumes/weather_data/silver2/weather_silver/",
    "silver_mobility": "/Volumes/weather_data/silver2/mobility_silver/",
}

# Great Expectations configuration paths
GE_CONFIG_PATH = "/dbfs/FileStore/shared_uploads/tests/"
GE_EXPECTATIONS_PATH = "/dbfs/FileStore/shared_uploads/tests/expectations/"
GE_CHECKPOINTS_PATH = "/dbfs/FileStore/shared_uploads/tests/checkpoints/"

# Data quality thresholds
QUALITY_THRESHOLDS = {
    "min_row_count": 1,
    "max_null_percentage": 0.1,  # 10% max nulls allowed
    "temperature_range": {"min": -50, "max": 60},  # Celsius for Greece
    "mobility_change_range": {"min": -100, "max": 500},  # Percentage change
    "coordinate_ranges": {
        "latitude": {"min": -90, "max": 90},
        "longitude": {"min": -180, "max": 180},
    },
}

# Expected columns for each layer
EXPECTED_COLUMNS = {
    "bronze_weather": [
        "latitude",
        "longitude",
        "daily",
        "source",
        "file_type",
        "ingestion_time",
    ],
    "bronze_mobility": [
        "date",
        "sub_region_1",
        "workplaces_percent_change_from_baseline",
        "transit_stations_percent_change_from_baseline",
        "parks_percent_change_from_baseline",
        "source",
        "file_type",
        "ingestion_time",
    ],
    "silver_weather": [
        "date",
        "temperature_max",
        "temperature_min",
        "precipitation_sum",
        "snowfall_sum",
        "wind_speed_max",
        "latitude",
        "longitude",
        "ingestion_time",
    ],
    "silver_mobility": [
        "date",
        "sub_region_1",
        "workplaces_percent_change_from_baseline",
        "transit_stations_percent_change_from_baseline",
        "parks_percent_change_from_baseline",
        "residential_percent_change_from_baseline",
        "source",
        "ingestion_time",
    ],
}

# Notification settings
NOTIFICATIONS = {
    "email_on_failure": False,
    "slack_on_failure": False,
    "log_all_results": True,
}

# Validation settings
VALIDATION_SETTINGS = {
    "stop_on_failure": False,  # Continue pipeline even if some tests fail
    "generate_docs": True,  # Generate data docs after validation
    "store_results": True,  # Store validation results
}
