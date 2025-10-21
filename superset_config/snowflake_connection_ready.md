# Snowflake Connection Script for Superset
# Copy-paste αυτά τα credentials στο Superset

# =================
# SNOWFLAKE CONNECTION READY TO USE
# =================

# 1. SUPERSET DATABASE CONNECTION
# Στο Superset UI: Data > Databases > + DATABASE
# Database Type: Snowflake
# SQLAlchemy URI (copy this exactly):

snowflake://AKARAGKIOZAKIS:Akaragkiozakis123@MPXFEBG-AP40156.snowflakecomputing.com/WEATHER_DWH/GOLD_LAYER?warehouse=COMPUTE_WH&role=ACCOUNTADMIN

# 2. CONNECTION DETAILS
Server: MPXFEBG-AP40156.snowflakecomputing.com
Username: AKARAGKIOZAKIS
Password: Akaragkiozakis123
Warehouse: COMPUTE_WH
Database: WEATHER_DWH
Schema: GOLD_LAYER
Role: ACCOUNTADMIN

# 3. EXPECTED TABLES IN SNOWFLAKE
# Μετά τη σύνδεση, αναμένουμε να βρούμε tables όπως:
# - WEATHER_GOLD_DAILY
# - MOBILITY_GOLD_DAILY  
# - WEATHER_MOBILITY_COMBINED
# - REGIONAL_WEATHER_SUMMARY

# 4. SAMPLE QUERY TO TEST CONNECTION
# Στο SQL Lab του Superset, δοκίμασε:
SELECT COUNT(*) FROM WEATHER_DWH.GOLD_LAYER.WEATHER_GOLD_DAILY;

# 5. QUICK SETUP CHECKLIST
# ✅ Copy URI above to Superset Database connection
# ✅ Test connection in Superset
# ✅ Create datasets from your Gold layer tables
# ✅ Start building charts and dashboards

# =================
# TROUBLESHOOTING
# =================
# Αν δεν συνδέεται, έλεγξε:
# 1. Snowflake account is active
# 2. User has correct permissions
# 3. Warehouse is running
# 4. Network connectivity to Snowflake
# 5. Superset has snowflake-sqlalchemy driver installed