import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

class DataGenerator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Sample data for user events
        self.event_types = ['page_view', 'click', 'purchase', 'login', 'logout', 'search']
        self.user_ids = [f'user_{i:03d}' for i in range(1, 101)]
        
        # Sample data for metrics
        self.metric_names = ['cpu_usage', 'memory_usage', 'response_time', 'error_rate', 'throughput']
        
    def generate_user_event(self) -> Dict[str, Any]:
        """Generate a random user event"""
        return {
            'user_id': random.choice(self.user_ids),
            'event_type': random.choice(self.event_types),
            'timestamp': datetime.now().isoformat(),
            'session_id': f'session_{random.randint(1000, 9999)}',
            'page_url': f'https://example.com/page_{random.randint(1, 50)}',
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'ip_address': f'192.168.1.{random.randint(1, 255)}'
        }
    
    def generate_metric(self) -> Dict[str, Any]:
        """Generate a random metric"""
        metric_name = random.choice(self.metric_names)
        
        if metric_name == 'cpu_usage':
            value = random.uniform(10, 95)
        elif metric_name == 'memory_usage':
            value = random.uniform(20, 90)
        elif metric_name == 'response_time':
            value = random.uniform(50, 500)
        elif metric_name == 'error_rate':
            value = random.uniform(0, 5)
        else:  # throughput
            value = random.uniform(100, 1000)
            
        return {
            'metric_name': metric_name,
            'metric_value': round(value, 2),
            'timestamp': datetime.now().isoformat(),
            'host': f'host-{random.randint(1, 10)}',
            'environment': random.choice(['production', 'staging', 'development'])
        }
    
    def send_user_events(self, count: int = 10):
        """Send user events to Kafka"""
        for i in range(count):
            event = self.generate_user_event()
            try:
                self.producer.send('user-events', event)
                print(f"Sent user event {i+1}/{count}: {event['event_type']} by {event['user_id']}")
                time.sleep(0.1)  # Small delay between messages
            except KafkaError as e:
                print(f"Failed to send user event: {e}")
    
    def send_metrics(self, count: int = 10):
        """Send metrics to Kafka"""
        for i in range(count):
            metric = self.generate_metric()
            try:
                self.producer.send('metrics', metric)
                print(f"Sent metric {i+1}/{count}: {metric['metric_name']} = {metric['metric_value']}")
                time.sleep(0.1)  # Small delay between messages
            except KafkaError as e:
                print(f"Failed to send metric: {e}")
    
    def run_continuous(self, interval_seconds: int = 5):
        """Run continuous data generation"""
        print(f"Starting continuous data generation every {interval_seconds} seconds...")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                # Send a few events and metrics
                self.send_user_events(random.randint(3, 8))
                self.send_metrics(random.randint(2, 5))
                
                print(f"Waiting {interval_seconds} seconds...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\nStopping data generation...")
        finally:
            self.producer.close()
    
    def close(self):
        """Close the Kafka producer"""
        self.producer.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate sample data for Kafka ETL pipeline')
    parser.add_argument('--mode', choices=['single', 'continuous'], default='single',
                       help='Data generation mode')
    parser.add_argument('--count', type=int, default=10,
                       help='Number of records to generate (single mode only)')
    parser.add_argument('--interval', type=int, default=5,
                       help='Interval between batches in seconds (continuous mode only)')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    generator = DataGenerator(args.bootstrap_servers)
    
    try:
        if args.mode == 'single':
            print(f"Generating {args.count} records...")
            generator.send_user_events(args.count // 2)
            generator.send_metrics(args.count // 2)
        else:
            generator.run_continuous(args.interval)
    finally:
        generator.close()



