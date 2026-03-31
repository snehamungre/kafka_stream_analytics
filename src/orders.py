import json
import random
import time
import uuid
from confluent_kafka import Producer

producer_config = {"bootstrap.servers": "localhost:9092"}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


# order = {"order_id": str(uuid.uuid4()), "user": "man", "item": "pen", "quantity": 1}

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
    }

    # Send data using order_id as the KEY
    producer.produce(
        topic="orders",
        key=order_id,
        value=json.dumps(data).encode("utf-8"),
        callback=delivery_report,
    )

    time.sleep(5)

producer.flush()
