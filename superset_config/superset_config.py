# Superset Configuration File
import os

# Database Configuration
# Χρησιμοποιεί το ίδιο PostgreSQL instance με το Airflow
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

# Secret Key για sessions
SECRET_KEY = "your_secret_key_here_change_in_production"

# Feature Flags
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_RBAC": True,
}

# Cache Configuration
CACHE_CONFIG = {
    "CACHE_TYPE": "simple",
}

# Supported Databases (για connections)
SQLALCHEMY_DATABASE_URI = os.getenv(
    "DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)

# Security
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365  # 1 year

# Internationalization
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "el": {"flag": "gr", "name": "Greek"},
}

# Additional Database Drivers
# Note: Import statements for optional drivers that may not be installed
# These are imported conditionally to avoid ImportError if drivers are missing

# Custom CSS (optional)
CUSTOM_CSS = """
.navbar-brand {
    font-weight: bold !important;
}
"""

# Logging
ENABLE_PROXY_FIX = True
LOG_LEVEL = "INFO"

# Email Configuration (για alerts - optional)
EMAIL_NOTIFICATIONS = False
SMTP_HOST = "localhost"
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = "admin"
SMTP_PORT = 587
SMTP_PASSWORD = "admin"
SMTP_MAIL_FROM = "admin@superset.com"

# ROW_LIMIT για queries
ROW_LIMIT = 5000
VIZ_ROW_LIMIT = 10000

# Enable API access
FAB_API_SWAGGER_UI = True
