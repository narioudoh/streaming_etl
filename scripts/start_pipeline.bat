@echo off
REM Real-time ETL Pipeline Startup Script for Windows
REM This script starts the entire pipeline with proper initialization

echo 🚀 Starting Real-time ETL Pipeline...

REM Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not running. Please start Docker and try again.
    pause
    exit /b 1
)

REM Check if Docker Compose is available
docker-compose --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker Compose is not installed. Please install it and try again.
    pause
    exit /b 1
)

REM Create data directory if it doesn't exist
if not exist "data" (
    echo 📁 Creating data directory...
    mkdir data
)

REM Stop any existing containers
echo 🛑 Stopping existing containers...
docker-compose down --remove-orphans

REM Build and start services
echo 🔨 Building and starting services...
docker-compose up -d --build

REM Wait for services to be ready
echo ⏳ Waiting for services to be ready...
timeout /t 30 /nobreak >nul

REM Check service status
echo 📊 Checking service status...
docker-compose ps

REM Wait for Kafka to be fully ready
echo ⏳ Waiting for Kafka to be fully ready...
timeout /t 20 /nobreak >nul

REM Create Kafka topics
echo 📝 Creating Kafka topics...
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic user-events
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic metrics
docker-compose exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic processed-data

REM List topics
echo 📋 Available Kafka topics:
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

REM Generate initial test data
echo 🎲 Generating initial test data...
docker-compose exec etl-service python data_generator.py --count 50

echo.
echo ✅ Pipeline is ready!
echo.
echo 🌐 Access the dashboard at: http://localhost:8501
echo 📊 ETL service running on: http://localhost:8000
echo 🔌 Kafka running on: localhost:9092
echo.
echo 📝 Useful commands:
echo   - View logs: docker-compose logs -f
echo   - Stop pipeline: docker-compose down
echo   - Generate data: docker-compose exec etl-service python data_generator.py --mode continuous
echo   - View topics: docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
echo.
echo 🎉 Happy streaming!
pause



