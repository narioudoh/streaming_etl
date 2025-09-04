#!/bin/bash

# Real-time ETL Pipeline Startup Script
# This script starts the entire pipeline with proper initialization

set -e

echo "🚀 Starting Real-time ETL Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fid

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install it and try again."
    exit 1
fi

# Create data directory if it doesn't exist
if [ ! -d "data" ]; then
    echo "📁 Creating data directory..."
    mkdir -p data
fi

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose down --remove-orphans

# Build and start services
echo "🔨 Building and starting services..."
docker-compose up -d --build

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check service status
echo "📊 Checking service status..."
docker-compose ps

# Wait for Kafka to be fully ready
echo "⏳ Waiting for Kafka to be fully ready..."
sleep 20

# Create Kafka topics
echo "📝 Creating Kafka topics..."
docker-compose exec kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic user-events

docker-compose exec kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic metrics

docker-compose exec kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic processed-data

# List topics
echo "📋 Available Kafka topics:"
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Generate initial test data
echo "🎲 Generating initial test data..."
docker-compose exec etl-service python data_generator.py --count 50

echo ""
echo "✅ Pipeline is ready!"
echo ""
echo "🌐 Access the dashboard at: http://localhost:8501"
echo "📊 ETL service running on: http://localhost:8000"
echo "🔌 Kafka running on: localhost:9092"
echo ""
echo "📝 Useful commands:"
echo "  - View logs: docker-compose logs -f"
echo "  - Stop pipeline: docker-compose down"
echo "  - Generate data: docker-compose exec etl-service python data_generator.py --mode continuous"
echo "  - View topics: docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092"
echo ""
echo "�� Happy streaming!"



