import json
import random
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import PRODUCER_DEFAULTS, SCHEMA_REGISTRY_CLIENT


producer_config = {
    **PRODUCER_DEFAULTS,
}

producer = Producer(producer_config)

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_CLIENT})

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})


with open("../schemas/order_schema_v2.avsc") as f:
    schema_str = f.read()


avro_serializer = AvroSerializer(schema_registry_client, schema_str)


for i in range(1, 101):
    order_id = str(uuid.uuid4())
    data = {
        "order_id": order_id,
        "user": f"user_{random.randint(1, 50)}",
        "item": random.choice(
            [
                "Laptop",
                "Mouse",
                "Keyboard",
                "Monitor",
                "Phone",
                "PC",
                "Wireless Headphones",
                "Wired Headphones",
            ]
        ),
        "price": random.randint(100, 5000),
        "quantity": random.randint(0, 5),
        "timestamp": time.time(),
        "category": "Electronics",
        "currency": "INR",
    }

    # Send data using order_id as the KEY
    producer.produce(
        topic="orders",
        key=order_id,
        value=avro_serializer(data, SerializationContext("orders", MessageField.VALUE)),
        callback=delivery_report,
    )

    time.sleep(5)

producer.flush()
