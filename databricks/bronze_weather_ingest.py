from datetime import datetime

from pyspark.sql.functions import current_timestamp, lit

# Input (raw files in bronze volume) and Output (Delta tables) paths
input_path = "/Volumes/weather_data/default/raw/"
bronze_json_path = "/Volumes/weather_data/default/jason/"
bronze_csv_path = "/Volumes/weather_data/default/csv/"


# Logging utility
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


try:
    files = dbutils.fs.ls(input_path)
    log_message("Bronze path exists")
except Exception as e:
    log_message(f"Bronze path not found: {e}")
    raise e

# Check if json files exist
json_files = [f for f in files if f.name.endswith(".json")]
if len(json_files) == 0:
    log_message("No JSON files found")
    json_exists = False
else:
    log_message(f"Found JSON files: {[f.name for f in json_files]}")
    json_exists = True

# Check if csv files exist
csv_files = [f for f in files if f.name.endswith(".csv")]
if len(csv_files) == 0:
    log_message("No CSV files found")
    csv_exists = False
else:
    log_message(f"Found CSV files: {[f.name for f in csv_files]}")
    csv_exists = True

# Process JSON files to Delta table
if json_exists:
    try:
        df_json = spark.read.option("multiline", "true").json(f"{input_path}*.json")
        log_message("Raw JSON files loaded successfully.")
        log_message("JSON schema:")
        df_json.printSchema()

        # Add metadata columns
        df_json = (
            df_json.withColumn("source", lit("open-meteo"))
            .withColumn("file_type", lit("json"))
            .withColumn("ingestion_time", current_timestamp())
        )

        # Save as Delta Table (Bronze Layer - JSON)
        df_json.write.format("delta").mode("overwrite").save(bronze_json_path)
        log_message(f"JSON Delta Table created at: {bronze_json_path}")

    except Exception as e:
        log_message(f"Error processing JSON data: {e}")
else:
    log_message("No JSON files found. JSON processing skipped.")

# Process CSV files to Delta table
if csv_exists:
    try:
        df_csv = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{input_path}*.csv")
        )
        log_message("Raw CSV files loaded successfully.")
        log_message("CSV schema:")
        df_csv.printSchema()

        # Add metadata columns
        df_csv = (
            df_csv.withColumn("source", lit("greece-mobility"))
            .withColumn("file_type", lit("csv"))
            .withColumn("ingestion_time", current_timestamp())
        )

        # Save as Delta Table (Bronze Layer - CSV)
        df_csv.write.format("delta").mode("overwrite").save(bronze_csv_path)
        log_message(f"CSV Delta Table created at: {bronze_csv_path}")

    except Exception as e:
        log_message(f"Error processing CSV data: {e}")
else:
    log_message("No CSV files found. CSV processing skipped.")

# Verification steps
if json_exists:
    try:
        bronze_json_df = spark.read.format("delta").load(bronze_json_path)
        json_count = bronze_json_df.count()
        log_message(f"JSON Delta table verification - total records: {json_count}")
        log_message("JSON Delta table sample:")
        display(bronze_json_df.limit(5))
    except Exception as e:
        log_message(f"Could not verify JSON Delta table: {e}")

if csv_exists:
    try:
        bronze_csv_df = spark.read.format("delta").load(bronze_csv_path)
        csv_count = bronze_csv_df.count()
        log_message(f"CSV Delta table verification - total records: {csv_count}")
        log_message("CSV Delta table sample:")
        display(bronze_csv_df.limit(5))
    except Exception as e:
        log_message(f"Could not verify CSV Delta table: {e}")

log_message("Bronze layer processing completed!")
