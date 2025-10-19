from pyspark.sql.functions import col, explode, to_timestamp, regexp_replace, count, when
from pyspark.sql.types import StringType, DoubleType, LongType
from datetime import datetime

# Paths
input_path = "/Volumes/raw/default/bronze/"
output_path = "/Volumes/raw/default/silver/"

# Logger
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Check if Bronze volume exists
try:
    dbutils.fs.ls(input_path)
    log_message("Found Bronze Volume.")
    volume_exists = True
except Exception as e:
    log_message(f"Bronze Volume not found or inaccessible: {e}")
    volume_exists = False

# Load and transform Bronze data
if volume_exists:
    try:
        # Load Bronze Delta table
        weather_data = spark.read.format("delta").load(input_path)
        log_message("Bronze data loaded successfully.")
        
        log_message("Bronze data schema:")
        weather_data.printSchema()

        # Explode nested array
        exploded_df = weather_data.select(
            explode(col("data")).alias("weather_record"),
            col("metadata.city").alias("city"),
            col("metadata.latitude").alias("latitude"),
            col("metadata.longitude").alias("longitude"),
            col("metadata.timezone").alias("timezone"),
            col("metadata.retrieved_at").alias("retrieved_at"),
            col("source").alias("data_source")
        )

        log_message("Data exploded successfully.")

        # Flatten structure
        flattened_df = exploded_df.select(
            col("city"),
            col("latitude"),
            col("longitude"),
            col("timezone"),
            col("retrieved_at"),
            col("data_source"),
            col("weather_record.time").alias("measurement_time"),
            col("weather_record.temperature_2m").alias("temperature_2m"),
            col("weather_record.relative_humidity_2m").alias("relative_humidity_2m"),
            col("weather_record.precipitation").alias("precipitation"),
            col("weather_record.rain").alias("rain"),
            col("weather_record.snowfall").alias("snowfall"),
            col("weather_record.wind_speed_10m").alias("wind_speed_10m"),
            col("weather_record.wind_gusts_10m").alias("wind_gusts_10m")
        )

        log_message("Data flattened successfully.")

        # Convert data types and clean timestamps
        converted_df = flattened_df.select(
            col("city").cast(StringType()).alias("city"),
            col("latitude").cast(DoubleType()).alias("latitude"),
            col("longitude").cast(DoubleType()).alias("longitude"),
            col("timezone").cast(StringType()).alias("timezone"),
            to_timestamp(
                regexp_replace(
                    regexp_replace(col("retrieved_at"), "T", " "),
                    r"\.\dZ$", ""
                )
            ).alias("retrieved_at"),
            col("data_source").cast(StringType()).alias("data_source"),
            to_timestamp(
                regexp_replace(col("measurement_time"), "T", " ")
            ).alias("measurement_time"),
            col("temperature_2m").cast(DoubleType()).alias("temperature_2m"),
            col("relative_humidity_2m").cast(LongType()).alias("relative_humidity_2m"),
            col("precipitation").cast(DoubleType()).alias("precipitation"),
            col("rain").cast(DoubleType()).alias("rain"),
            col("snowfall").cast(DoubleType()).alias("snowfall"),
            col("wind_speed_10m").cast(DoubleType()).alias("wind_speed_10m"),
            col("wind_gusts_10m").cast(DoubleType()).alias("wind_gusts_10m")
)

        log_message("Data types converted successfully.")
        log_message("Final Silver data schema:")
        converted_df.printSchema()

        # Check for null values
        log_message("Checking for null values in each column...")
        null_counts = (
            converted_df.select([
                count(when(col(c).isNull(), c)).alias(c)
                for c in converted_df.columns
            ])
        )
        display(null_counts)

        # Show sample data
        log_message("Sample transformed data:")
        display(converted_df.limit(10))

        # Record count
        record_count = converted_df.count()
        log_message(f"Total records after transformation: {record_count}")

        # Save to Silver Volume
        try:
            converted_df.write.format("delta").mode("overwrite").save(output_path)
            log_message(f"Silver Delta table created successfully at: {output_path}")
            
            silver_verification = spark.read.format("delta").load(output_path)
            verification_count = silver_verification.count()
            log_message(f"Verification: Silver table contains {verification_count} records.")
            
        except Exception as e:
            log_message(f"Error saving Silver table: {e}")

    except Exception as e:
        log_message(f"Error during transformation: {e}")
        raise e

else:
    log_message("Skipping transformation since Bronze Volume was not found.")

log_message("Silver layer transformation completed.")
