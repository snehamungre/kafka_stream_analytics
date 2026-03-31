from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "validation",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)

        required_fields = [
            "order_id",
            "user",
            "item",
            "category",
            "quantity",
            "timestamp",
            "price",
        ]
        missing_fields = [f for f in required_fields if f not in order]

        if missing_fields:
            print(f"❌ INVALID order: Missing {missing_fields}")
            continue

        if order["quantity"] <= 0:
            print(
                f"❌ INVALID order: quantity must be greater than 0 and not {order["quantity"]}"
            )
            continue

        print(f"✅ VALID: Order {order['order_id']} is good!")

except KeyboardInterrupt:
    print("\n🔴 Stopping Validation Service")

finally:
    consumer.close()
