from botocore.client import Config
import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Kafka consumer settings
consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    try:
        s3.head_bucket(Bucket=bucket)
    except:
        s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# Batch consume
batch_size = 50
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

# -----------------------------
# Debugging: Check Kafka setup
# -----------------------------
print("🔍 Checking Kafka connection...")
print(f"Bootstrap servers: {os.getenv('KAFKA_BOOTSTRAP')}")
print(f"Consumer group: {os.getenv('KAFKA_GROUP')}")

# Wait for partition assignment
consumer.poll(timeout_ms=1000)

print(f"\n📋 Subscribed topics: {consumer.subscription()}")
print(f"📊 Assigned partitions: {consumer.assignment()}")

# Check current offsets
if consumer.assignment():
    for partition in consumer.assignment():
        try:
            position = consumer.position(partition)
            print(f"   Partition {partition}: current offset = {position}")
        except:
            print(f"   Partition {partition}: offset not available yet")

print("\n✅ Connected to Kafka. Listening for messages...\n")

# -----------------------------
# Main consumption loop
# -----------------------------
try:
    message_count = 0
    
    while True:
        # Poll with timeout to avoid indefinite blocking
        msg_batch = consumer.poll(timeout_ms=5000)
        
        if not msg_batch:
            print("⏳ No messages received in last 5 seconds... (still listening)")
            continue
        
        # Process messages from all partitions
        for topic_partition, messages in msg_batch.items():
            for message in messages:
                message_count += 1
                topic = message.topic
                event = message.value
                
                # Extract the actual record from Debezium CDC format
                payload = event.get("payload", {})
                record = payload.get("after")  # Only take the actual row
                
                if record:
                    buffer[topic].append(record)
                    print(f"[{message_count}] [{topic}] -> {record}")
                else:
                    # Handle delete operations (after is null)
                    operation = payload.get("op")
                    if operation == "d":
                        print(f"[{message_count}] [{topic}] DELETE operation detected")
                    else:
                        print(f"[{message_count}] [{topic}] No 'after' field in payload")
                
                # Write batch to MinIO when buffer is full
                if len(buffer[topic]) >= batch_size:
                    write_to_minio(topic.split('.')[-1], buffer[topic])
                    buffer[topic] = []

except KeyboardInterrupt:
    print("\n\n🛑 Shutting down consumer gracefully...")

except Exception as e:
    print(f"\n❌ Error occurred: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Flush any remaining records in buffers
    print("\n📤 Flushing remaining records...")
    for topic, records in buffer.items():
        if records:
            table_name = topic.split('.')[-1]
            write_to_minio(table_name, records)
            print(f"   Flushed {len(records)} records from {table_name}")
    
    consumer.close()
    print("✅ Consumer closed. Goodbye!")