from datetime import datetime

from pyspark.sql.functions import arrays_zip, col, count, explode, when
from pyspark.sql.types import DateType, DoubleType, TimestampType

# Paths
input_json_path = "/Volumes/weather_data/default/jason/"
input_csv_path = "/Volumes/weather_data/default/csv/"
weather_silver_path = "/Volumes/weather_data/silver2/weather_silver/"
mobility_silver_path = "/Volumes/weather_data/silver2/mobility_silver/"

# Initialize DataFrames
converted_df = None
clean_df = None


# Logger
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


# Check if JSON volume exists
try:
    dbutils.fs.ls(input_json_path)
    log_message("✅ Found JSON (jason) Volume.")
    jason_volume_exists = True
except Exception as e:
    log_message(f"⚠️ JSON Volume not found or inaccessible: {e}")
    jason_volume_exists = False

# Check if CSV volume exists
try:
    dbutils.fs.ls(input_csv_path)
    log_message("✅ Found CSV volume.")
    csv_volume_exists = True
except Exception as e:
    log_message(f"⚠️ CSV Volume not found or inaccessible: {e}")
    csv_volume_exists = False

# -----------------------------
# WEATHER (JSON) → SILVER TRANSFORMATION
# -----------------------------
if jason_volume_exists:
    try:
        # Load Bronze Delta table
        weather_data = spark.read.format("delta").load(input_json_path)
        log_message("🌤 JSON data loaded successfully.")

        log_message("🔍 JSON data schema:")
        weather_data.printSchema()

        # Explode nested arrays properly
        exploded_df = weather_data.select(
            explode(
                arrays_zip(
                    col("daily.time"),
                    col("daily.temperature_2m_max"),
                    col("daily.temperature_2m_min"),
                    col("daily.precipitation_sum"),
                    col("daily.snowfall_sum"),
                    col("daily.wind_speed_10m_max"),
                )
            ).alias("weather_record"),
            col("latitude"),
            col("longitude"),
            col("source"),
            col("ingestion_time"),
        )

        log_message("✅ Data exploded successfully.")

        # Flatten structure
        flattened_df = exploded_df.select(
            col("weather_record.time").alias("date"),
            col("weather_record.temperature_2m_max").alias("temperature_max"),
            col("weather_record.temperature_2m_min").alias("temperature_min"),
            col("weather_record.precipitation_sum").alias("precipitation_sum"),
            col("weather_record.snowfall_sum").alias("snowfall_sum"),
            col("weather_record.wind_speed_10m_max").alias("wind_speed_max"),
            col("latitude"),
            col("longitude"),
            col("source"),
            col("ingestion_time"),
        )

        log_message("✅ Data flattened successfully.")

        # Convert data types and clean timestamps
        converted_df = flattened_df.select(
            col("date").cast(DateType()).alias("date"),
            col("latitude").cast(DoubleType()).alias("latitude"),
            col("longitude").cast(DoubleType()).alias("longitude"),
            col("ingestion_time").cast(TimestampType()).alias("ingestion_time"),
            col("temperature_max").cast(DoubleType()).alias("temperature_max"),
            col("temperature_min").cast(DoubleType()).alias("temperature_min"),
            col("precipitation_sum").cast(DoubleType()).alias("precipitation_sum"),
            col("snowfall_sum").cast(DoubleType()).alias("snowfall_sum"),
            col("wind_speed_max").cast(DoubleType()).alias("wind_speed_max"),
        )

        log_message("✅ Data types converted successfully.")
        log_message("📘 Final Silver weather data schema:")
        converted_df.printSchema()

        # Check for null values
        log_message("🔎 Checking for null values in weather data...")
        null_counts = converted_df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in converted_df.columns]
        )
        display(null_counts)

        # Show sample data
        log_message("📊 Sample transformed weather data:")
        display(converted_df.limit(10))

        # Record count
        record_count = converted_df.count()
        log_message(f"✅ Total weather records after transformation: {record_count}")

    except Exception as e:
        log_message(f"❌ Error during JSON Silver transformation: {e}")
        raise e
else:
    log_message("⚠️ JSON volume not found. Weather Silver transformation skipped.")

# -----------------------------
# MOBILITY (CSV) → SILVER TRANSFORMATION
# -----------------------------
if csv_volume_exists:
    try:
        mobility_data = spark.read.format("delta").load(input_csv_path)
        log_message("✅ CSV data loaded successfully.")

        log_message("🔍 CSV data schema:")
        mobility_data.printSchema()

        log_message("🔎 Checking for null values in mobility data...")
        null_counts = mobility_data.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in mobility_data.columns]
        )
        display(null_counts)

        # Clearing null values and dropping unnecessary columns
        clean_df = mobility_data.dropna(subset=["sub_region_1"])
        clean_df = clean_df.drop(
            "sub_region_2", "metro_area", "iso_3166_2_code", "census_fips_code"
        )
        log_message("✅ Null values cleared and specific columns dropped successfully.")

        log_message("📘 Final Silver mobility data schema:")
        clean_df.printSchema()

        # Record count
        mobility_record_count = clean_df.count()
        log_message(
            f"✅ Total mobility records after transformation: {mobility_record_count}"
        )

    except Exception as e:
        log_message(f"❌ Error during CSV Silver transformation: {e}")
        raise e
else:
    log_message("⚠️ CSV volume not found. Mobility Silver transformation skipped.")

# -----------------------------
# PREVIEW DATA BEFORE SAVING
# -----------------------------
if converted_df is not None:
    log_message("📊 Previewing 100 sample weather rows before saving to Silver...")
    display(converted_df.limit(100))

if clean_df is not None:
    log_message("📊 Previewing 100 sample mobility rows before saving to Silver...")
    display(clean_df.limit(100))

# -----------------------------
# SAVE TO SILVER LAYER
# -----------------------------

# Save Weather (JSON) to Silver
if converted_df is not None:
    try:
        converted_df.write.format("delta").mode("overwrite").save(weather_silver_path)
        log_message(
            f"✅ Weather Silver Delta table created successfully at: {weather_silver_path}"
        )

        silver_verification = spark.read.format("delta").load(weather_silver_path)
        verification_count = silver_verification.count()
        log_message(
            f"🔍 Verification: Weather Silver table contains {verification_count} records."
        )

    except Exception as e:
        log_message(f"❌ Error saving Weather Silver table: {e}")
else:
    log_message("⚠️ No weather data to save to Silver layer.")

# Save Mobility (CSV) to Silver
if clean_df is not None:
    try:
        clean_df.write.format("delta").mode("overwrite").save(mobility_silver_path)
        log_message(
            f"✅ Mobility Silver Delta table created successfully at: {mobility_silver_path}"
        )

        silver_verification = spark.read.format("delta").load(mobility_silver_path)
        verification_count = silver_verification.count()
        log_message(
            f"🔍 Verification: Mobility Silver table contains {verification_count} records."
        )

    except Exception as e:
        log_message(f"❌ Error saving Mobility Silver table: {e}")
else:
    log_message("⚠️ No mobility data to save to Silver layer.")

log_message("🎉 Silver layer transformation completed!")
