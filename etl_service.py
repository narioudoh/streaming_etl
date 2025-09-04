import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.duckdb_path = os.getenv('DATA_PATH')
        
        # Initialize Kafka consumer and producer
        self.consumer = KafkaConsumer(
            ['user-events', 'metrics'],  # Listen to both topics
            bootstrap_servers=self.kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='etl-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Initialize DuckDB
        self.init_database()
        
    def init_database(self):
        """Initialize DuckDB database and create tables"""
        try:
            self.db = duckdb.connect(self.duckdb_path)
            
            # Create tables for different data types
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS user_events (
                    id INTEGER PRIMARY KEY,
                    user_id VARCHAR,
                    event_type VARCHAR,
                    timestamp TIMESTAMP,
                    data JSON,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY,
                    metric_name VARCHAR,
                    metric_value DOUBLE,
                    timestamp TIMESTAMP,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS processed_data (
                    id INTEGER PRIMARY KEY,
                    source_topic VARCHAR,
                    record_count INTEGER,
                    processing_time_ms DOUBLE,
                    timestamp TIMESTAMP,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def process_user_event(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process user event data with Pandas"""
        try:
            # Convert to DataFrame for processing
            df = pd.DataFrame([data])
            
            # Add processing timestamp
            df['processed_at'] = pd.Timestamp.now()
            
            # Clean and validate data
            df['user_id'] = df['user_id'].astype(str)
            df['event_type'] = df['event_type'].str.lower()
            
            # Add derived fields
            df['hour_of_day'] = pd.to_datetime(df['timestamp']).dt.hour
            df['day_of_week'] = pd.to_datetime(df['timestamp']).dt.day_name()
            
            # Convert back to dict
            processed_data = df.to_dict('records')[0]
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing user event: {e}")
            return data
    
    def process_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process metrics data with Pandas"""
        try:
            df = pd.DataFrame([data])
            
            # Add processing timestamp
            df['processed_at'] = pd.Timestamp.now()
            
            # Validate numeric values
            df['metric_value'] = pd.to_numeric(df['metric_value'], errors='coerce')
            
            # Add derived fields
            df['is_anomaly'] = df['metric_value'] > df['metric_value'].mean() + 2 * df['metric_value'].std()
            
            processed_data = df.to_dict('records')[0]
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing metrics: {e}")
            return data
    
    def store_data(self, data: Dict[str, Any], table_name: str):
        """Store processed data in DuckDB"""
        try:
            if table_name == 'user_events':
                self.db.execute("""
                    INSERT INTO user_events (user_id, event_type, timestamp, data)
                    VALUES (?, ?, ?, ?)
                """, (data['user_id'], data['event_type'], data['timestamp'], json.dumps(data)))
                
            elif table_name == 'metrics':
                self.db.execute("""
                    INSERT INTO metrics (metric_name, metric_value, timestamp)
                    VALUES (?, ?, ?)
                """, (data['metric_name'], data['metric_value'], data['timestamp']))
            
            # Store processing metadata
            self.db.execute("""
                INSERT INTO processed_data (source_topic, record_count, processing_time_ms, timestamp)
                VALUES (?, ?, ?, ?)
            """, (table_name, 1, 0, datetime.now()))
            
            logger.info(f"Data stored in {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to store data in {table_name}: {e}")
    
    def run(self):
        """Main ETL processing loop"""
        logger.info("Starting ETL pipeline...")
        
        try:
            for message in self.consumer:
                start_time = time.time()
                
                try:
                    data = message.value
                    topic = message.topic
                    
                    logger.info(f"Processing message from topic: {topic}")
                    
                    # Process data based on topic
                    if topic == 'user-events':
                        processed_data = self.process_user_event(data)
                        self.store_data(processed_data, 'user_events')
                        
                    elif topic == 'metrics':
                        processed_data = self.process_metrics(data)
                        self.store_data(processed_data, 'metrics')
                    
                    # Send processed data to output topic
                    self.producer.send('processed-data', {
                        'original_topic': topic,
                        'processed_data': processed_data,
                        'processing_time': time.time() - start_time
                    })
                    
                    processing_time = (time.time() - start_time) * 1000
                    logger.info(f"Message processed in {processing_time:.2f}ms")
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Shutting down ETL pipeline...")
        finally:
            self.consumer.close()
            self.producer.close()
            self.db.close()

if __name__ == "__main__":
    # Wait for Kafka to be ready
    time.sleep(10)
    
    etl = ETLPipeline()
    etl.run()

