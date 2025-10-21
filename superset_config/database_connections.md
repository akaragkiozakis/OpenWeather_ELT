# Superset Database Connections Configuration
# Αυτό το αρχείο περιέχει samples για database connections

# =================
# DATABRICKS CONNECTION
# =================
# Database Type: Databricks
# SQLAlchemy URI Format:
# databricks+connector://token:<access_token>@<databricks_host>:443/<database_name>?http_path=<http_path>

# Example:
# databricks+connector://token:dapi1234567890abcdef@your-workspace.cloud.databricks.com:443/weather_data?http_path=/sql/1.0/warehouses/your_warehouse_id

# Steps to connect:
# 1. Στο Databricks: Compute > SQL Warehouses > Server hostname & HTTP path
# 2. User Settings > Developer > Access Tokens > Generate new token
# 3. Στο Superset: Data > Databases > + DATABASE
# 4. Choose "Databricks" και βάλε το URI

# =================
# SNOWFLAKE CONNECTION  
# =================
# Database Type: Snowflake
# SQLAlchemy URI Format:
# snowflake://<username>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>&role=<role>

# Example:
# snowflake://your_user:your_password@your_account.snowflakecomputing.com/WEATHER_DB/PUBLIC?warehouse=COMPUTE_WH&role=ACCOUNTADMIN

# Steps to connect:
# 1. Snowflake account details (account, username, password)
# 2. Database, Schema, Warehouse, Role names
# 3. Στο Superset: Data > Databases > + DATABASE  
# 4. Choose "Snowflake" και βάλε το URI

# =================
# LOCAL POSTGRESQL (για testing)
# =================
# Database Type: PostgreSQL
# SQLAlchemy URI: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow

# =================
# SAMPLE DATASETS
# =================
# Μετά τη σύνδεση, δημιούργησε datasets:
# 1. weather_silver (από Databricks Silver layer)
# 2. mobility_silver (από Databricks Silver layer)  
# 3. gold_weather_mobility_daily (από Gold layer)

# =================
# CONNECTION TESTING
# =================
# Για να ελέγξεις τη σύνδεση:
# 1. Data > Databases > Test Connection
# 2. SQL Lab > SQL Editor > Τρέξε SELECT 1
# 3. Charts > Create new chart > επιλογή dataset