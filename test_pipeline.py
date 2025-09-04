#!/usr/bin/env python3
"""
Test script for the Real-time ETL Pipeline
This script tests the data flow from Kafka through the ETL service to DuckDB
"""

import json
import time
import duckdb
import os
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

def test_kafka_connection(bootstrap_servers='localhost:9092'):
    """Test Kafka connectivity"""
    print("🔌 Testing Kafka connection...")
    
    try:
        # Test producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Test consumer
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        producer.close()
        consumer.close()
        
        print("✅ Kafka connection successful!")
        return True
        
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False

def test_data_generation(bootstrap_servers='localhost:9092'):
    """Test data generation and sending to Kafka"""
    print("🎲 Testing data generation...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Generate test data
        test_events = [
            {
                'user_id': 'test_user_001',
                'event_type': 'test_event',
                'timestamp': datetime.now().isoformat(),
                'test_data': 'pipeline_test'
            },
            {
                'user_id': 'test_user_002',
                'event_type': 'test_event',
                'timestamp': datetime.now().isoformat(),
                'test_data': 'pipeline_test'
            }
        ]
        
        # Send test data
        for event in test_events:
            producer.send('user-events', event)
            print(f"📤 Sent test event: {event['user_id']}")
        
        producer.flush()
        producer.close()
        
        print("✅ Data generation test successful!")
        return True
        
    except Exception as e:
        print(f"❌ Data generation test failed: {e}")
        return False

def test_etl_processing(bootstrap_servers='localhost:9092'):
    """Test ETL processing by consuming from processed-data topic"""
    print("⚙️  Testing ETL processing...")
    
    try:
        consumer = KafkaConsumer(
            'processed-data',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000
        )
        
        # Wait for processed data
        messages = list(consumer)
        consumer.close()
        
        if messages:
            print(f"✅ ETL processing test successful! Received {len(messages)} processed messages")
            for msg in messages[:2]:  # Show first 2 messages
                data = json.loads(msg.value.decode('utf-8'))
                print(f"   📊 Processed: {data.get('original_topic', 'unknown')}")
            return True
        else:
            print("❌ No processed data received from ETL service")
            return False
            
    except Exception as e:
        print(f"❌ ETL processing test failed: {e}")
        return False

def test_database_storage():
    """Test data storage in DuckDB"""
    print("🗄️  Testing database storage...")
    
    try:
        duckdb_path = os.getenv('DUCKDB_PATH', '/data/etl_data.duckdb')
        
        if not os.path.exists(duckdb_path):
            print(f"❌ Database file not found at {duckdb_path}")
            return False
        
        # Connect to database
        db = duckdb.connect(duckdb_path)
        
        # Check tables
        tables = db.execute("SHOW TABLES").fetchdf()
        print(f"📋 Found {len(tables)} tables: {list(tables['name'])}")
        
        # Check data in user_events table
        user_events_count = db.execute("SELECT COUNT(*) FROM user_events").fetchone()[0]
        print(f"👥 User events count: {user_events_count}")
        
        # Check data in metrics table
        metrics_count = db.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        print(f"📊 Metrics count: {metrics_count}")
        
        # Check recent data
        recent_events = db.execute("""
            SELECT user_id, event_type, timestamp 
            FROM user_events 
            ORDER BY timestamp DESC 
            LIMIT 3
        """).fetchdf()
        
        if not recent_events.empty:
            print("📝 Recent user events:")
            for _, event in recent_events.iterrows():
                print(f"   - {event['user_id']}: {event['event_type']} at {event['timestamp']}")
        
        db.close()
        
        if user_events_count > 0 or metrics_count > 0:
            print("✅ Database storage test successful!")
            return True
        else:
            print("❌ No data found in database")
            return False
            
    except Exception as e:
        print(f"❌ Database storage test failed: {e}")
        return False

def test_end_to_end_pipeline():
    """Test the complete end-to-end pipeline"""
    print("🔄 Testing end-to-end pipeline...")
    
    # Wait for ETL service to process data
    print("⏳ Waiting for ETL processing...")
    time.sleep(15)
    
    # Run all tests
    tests = [
        ("Kafka Connection", test_kafka_connection),
        ("Data Generation", test_data_generation),
        ("ETL Processing", test_etl_processing),
        ("Database Storage", test_database_storage)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running: {test_name}")
        print('='*50)
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{'='*50}")
    print("TEST RESULTS SUMMARY")
    print('='*50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Pipeline is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Check the logs for details.")
        return False

def main():
    """Main test function"""
    print("🧪 Real-time ETL Pipeline Test Suite")
    print("="*50)
    
    # Check if we're running in Docker
    if os.path.exists('/.dockerenv'):
        print("🐳 Running inside Docker container")
        bootstrap_servers = 'kafka:9092'
    else:
        print("💻 Running on host machine")
        bootstrap_servers = 'localhost:9092'
    
    print(f"🔌 Using Kafka at: {bootstrap_servers}")
    print()
    
    # Run end-to-end test
    success = test_end_to_end_pipeline()
    
    if success:
        print("\n🎉 Pipeline test completed successfully!")
        print("🌐 You can now access the dashboard at: http://localhost:8501")
    else:
        print("\n❌ Pipeline test failed!")
        print("🔍 Check the logs and ensure all services are running")
        print("💡 Try running: docker-compose logs -f")
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())



