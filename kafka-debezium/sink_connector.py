import os
import sys
import time
import io
import boto3
import json
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# List of topics to consume 
TOPICS = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.customers_accounts',
    'banking_server.public.transactions',
]
# Number of messages to process in each batch
BATCH_SIZE = 5
# S3 Bucket name
BUCKET_NAME = os.getenv("MINIO_BUCKET")
# Initialize connections
print("Connecting to MinIO...")
try:
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"), 
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )
    # create bucket if not exists
    if BUCKET_NAME not in [b["Name"] for b in s3.list_buckets()['Buckets']]:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created.")
    else:
        print(f"Bucket '{BUCKET_NAME}' found.")
except Exception as e:
    print(f"Error connecting to MinIO: {e}")
    sys.exit(1)

print("Connecting to Kafka...")
try:
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
        auto_offset_reset='earliest', # start from the beginning if no offset is committed
        enable_auto_commit=True,
        group_id='minio-sink-connector-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Successfully connected to Kafka.")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    sys.exit(1)

# Process messages in batches and upload to MinIO
buffer = {topic: [] for topic in TOPICS}

def upload_parquet_to_minio(topic, records):
    """Convert records to Parquet format and upload to MinIO."""
    if not records:
        return
    df = pd.DataFrame(records)
    table_name = topic.split('.')[-1]
    now = datetime.now()
    date_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    file_name = f"{table_name}_{now.strftime('%H%M%S%f')}.parquet"
    s3_key = f"{table_name}/{date_path}/{file_name}"

    # Convert to Parquet (In-memory buffer)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
    parquet_buffer.seek(0)
    # Upload to MinIO
    try:
        s3.upload_fileobj(parquet_buffer, BUCKET_NAME, s3_key)
        print(f"Uploaded {len(records)} rows to s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")

# Main loop to consume messages
print("Listening for messages...")
try:
    for msg in consumer:
        topic = msg.topic
        event = msg.value
        payload = event.get('payload', {})
        if not payload:
            continue
        operation = payload.get('op') # (c=create, u=update, d=delete, r=read)
        record = payload.get('after')

        if record:
            # Add addtional metadata
            record['_op'] = operation
            record['_ts_ms'] = payload.get('ts_ms')

            buffer[topic].append(record)
        if len(buffer[topic]) >= BATCH_SIZE:
            print(f"Processing batch for topic {topic}...")
            upload_parquet_to_minio(topic, buffer[topic])
            buffer[topic] = []
except KeyboardInterrupt:
    print("Stopping consumer...")
    # Flush remaining records
    for topic, records in buffer.items():
        if records:
            print(f"Flushing remaining {len(records)} records for topic {topic}...")
            upload_parquet_to_minio(topic, records)
    print("Consumer stopped.")