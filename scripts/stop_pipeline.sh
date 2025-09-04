#!/bin/bash

# Real-time ETL Pipeline Shutdown Script
# This script properly stops the pipeline and cleans up resources

echo "🛑 Stopping Real-time ETL Pipeline..."

# Stop all services
echo "⏹️  Stopping Docker containers..."
docker-compose down --remove-orphans

# Remove any dangling containers
echo "🧹 Cleaning up dangling containers..."
docker container prune -f

# Remove any dangling networks
echo "🧹 Cleaning up dangling networks..."
docker network prune -f

# Remove any dangling volumes (optional - uncomment if you want to remove data)
# echo "🧹 Cleaning up dangling volumes..."
# docker volume prune -f

echo ""
echo "✅ Pipeline stopped successfully!"
echo ""
echo "💡 To start the pipeline again, run: ./scripts/start_pipeline.sh"
echo "💡 To completely reset (including data), run: docker-compose down -v && docker system prune -f"



