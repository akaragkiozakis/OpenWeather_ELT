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

# YOUR ACTUAL CONNECTION:
snowflake://AKARAGKIOZAKIS:Akaragkiozakis123@MPXFEBG-AP40156.snowflakecomputing.com/WEATHER_DWH/GOLD_LAYER?warehouse=COMPUTE_WH&role=ACCOUNTADMIN

# Connection Details:
# Server: MPXFEBG-AP40156.snowflakecomputing.com
# Warehouse: COMPUTE_WH
# Database: WEATHER_DWH
# Schema: GOLD_LAYER
# Role: ACCOUNTADMIN
# Username: AKARAGKIOZAKIS
# Password: Akaragkiozakis123

# Steps to connect:
# 1. Στο Superset: Data > Databases > + DATABASE  
# 2. Choose "Snowflake" 
# 3. Copy-paste το παραπάνω URI στο "SQLAlchemy URI" field
# 4. Click "Test Connection" για έλεγχο
# 5. Save

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