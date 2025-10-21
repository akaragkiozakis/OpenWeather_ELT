#!/bin/bash

# Complete Docker Stack Startup Script
echo "🐳 Starting Weather ELT Pipeline Stack..."
echo "================================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

echo "✅ Docker is running"

# Navigate to docker directory
cd "$(dirname "$0")"

# Create necessary directories
mkdir -p ../dags ../logs ../plugins

# Set Airflow UID (for Linux/macOS)
export AIRFLOW_UID=$(id -u) 2>/dev/null || export AIRFLOW_UID=50000

echo "🚀 Starting services..."

# Start all services
docker-compose up -d

echo "⏳ Waiting for services to be ready..."

# Wait for Airflow
echo "🌪️ Waiting for Airflow..."
timeout=300
counter=0
while ! curl -s http://localhost:8080/health > /dev/null; do
    sleep 5
    counter=$((counter + 5))
    if [ $counter -ge $timeout ]; then
        echo "❌ Airflow startup timeout"
        break
    fi
done

# Wait for Superset
echo "📊 Waiting for Superset..."
counter=0
while ! curl -s http://localhost:8088/health > /dev/null; do
    sleep 5
    counter=$((counter + 5))
    if [ $counter -ge $timeout ]; then
        echo "❌ Superset startup timeout"
        break
    fi
done

echo "================================================"
echo "🎉 Stack is ready! Access your services:"
echo ""
echo "🌪️  Airflow:   http://localhost:8080"
echo "   👤 Username: admin"
echo "   🔑 Password: admin"
echo ""
echo "📊 Superset:  http://localhost:8088"  
echo "   👤 Username: admin"
echo "   🔑 Password: admin"
echo ""
echo "💾 PostgreSQL: localhost:5432"
echo "   👤 Username: airflow"
echo "   🔑 Password: airflow"
echo "   🗄️ Database: airflow"
echo ""
echo "📋 To view logs: docker-compose logs -f [service_name]"
echo "🛑 To stop all:  docker-compose down"
echo "================================================"