#!/bin/bash

# Superset Startup Script
echo "🚀 Starting Apache Superset..."

# Wait for database to be ready
echo "⏳ Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "✅ PostgreSQL is ready!"

# Create admin user if not exists
echo "👤 Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@superset.com \
    --password admin || echo "Admin user already exists"

# Initialize database
echo "🗄️ Initializing Superset database..."
superset db upgrade

# Initialize Superset
echo "⚙️ Initializing Superset..."
superset init

# Load example data (optional)
# superset load_examples

echo "🌟 Superset is ready!"
echo "📍 Access at: http://localhost:8088"
echo "👤 Username: admin"
echo "🔑 Password: admin"

# Start Superset
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger