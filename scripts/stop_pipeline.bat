@echo off
REM Real-time ETL Pipeline Shutdown Script for Windows
REM This script properly stops the pipeline and cleans up resources

echo 🛑 Stopping Real-time ETL Pipeline...

REM Stop all services
echo ⏹️  Stopping Docker containers...
docker-compose down --remove-orphans

REM Remove any dangling containers
echo 🧹 Cleaning up dangling containers...
docker container prune -f

REM Remove any dangling networks
echo 🧹 Cleaning up dangling networks...
docker network prune -f

REM Remove any dangling volumes (optional - uncomment if you want to remove data)
REM echo 🧹 Cleaning up dangling volumes...
REM docker volume prune -f

echo.
echo ✅ Pipeline stopped successfully!
echo.
echo 💡 To start the pipeline again, run: scripts\start_pipeline.bat
echo 💡 To completely reset (including data), run: docker-compose down -v && docker system prune -f
pause



