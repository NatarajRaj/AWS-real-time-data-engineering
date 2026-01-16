import json
import random
import time
import boto3
from faker import Faker
from datetime import datetime

fake = Faker()
kinesis = boto3.client("kinesis", region_name="ap-south-1")

STREAM_NAME = "transaction-stream"

def generate_event():
    return {
        "transaction_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "amount": round(random.uniform(10, 6000), 2),
        "currency": "INR",
        "status": random.choice(["SUCCESS", "FAILED"]),
        "event_time": datetime.utcnow().isoformat()
    }

while True:
    event = generate_event()

    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event),
        PartitionKey=event["user_id"]
    )

    print("Sent:", event)
    time.sleep(1)
