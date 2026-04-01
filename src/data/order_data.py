import random
import time
import uuid


def generate_order():
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
        "quantity": random.randint(1, 5),
        "timestamp": time.time(),
        "category": "Electronics",
        "currency": "INR",
    }

    return data
