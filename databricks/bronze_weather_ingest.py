from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime

# Input (raw JSON) and Output (Bronze Delta) paths
input_path = "/Volumes/raw/default/weather_data/athens-2025-present.json"
bronze_path = "/Volumes/raw/default/bronze/"


# Logging utility
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Check if input file exists in DBFS
try:
    files = [f.name for f in dbutils.fs.ls("/Volumes/raw/default/weather_data/")]
    if "athens-2025-present.json" in files:
        log_message("Found input JSON file in Volume.")
        file_exists = True
    else:
        log_message("JSON file not found in /Volumes/raw/default/weather_data/.")
        file_exists = False
except Exception as e:
    log_message(f"Error checking Volume: {e}")
    file_exists = False


# Load and transform data
if file_exists:
    try:
        df = spark.read.option("multiline", "true").json(input_path)
        log_message("Raw JSON file loaded successfully.")
        df.printSchema()

        # Add metadata columns
        df = (
            df.withColumn("source", lit("open-meteo"))
              .withColumn("ingestion_time", current_timestamp())
        )

        # Save as Delta Table (Bronze Layer)
        df.write.format("delta").mode("overwrite").save(bronze_path)
        log_message(f"Bronze Delta Table created at: {bronze_path}")

    except Exception as e:
        log_message(f"Error processing JSON data: {e}")
else:
    log_message("No file found. Bronze process skipped.")

# Verification step
try:
    bronze_df = spark.read.format("delta").load(bronze_path)
    log_message(f"Bronze table verification - total records: {bronze_df.count()}")
    display(bronze_df.limit(5))
    bronze_df.printSchema()
except Exception as e:
    log_message(f"Could not verify Bronze table: {e}")
