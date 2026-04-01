import json
import random
from config import CONSUMER_DEFAULTS, PRODUCER_DEFAULTS
from confluent_kafka import Consumer, Producer

consumer_config = {
    **CONSUMER_DEFAULTS,
    "group.id": "payments",
}

producer_config = {
    **PRODUCER_DEFAULTS,
}


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


producer = Producer(producer_config)
consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("Payments is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)

        total = order["price"] * order["quantity"]

        order["total_price"] = total

        payment_success = random.random() < 0.8

        if payment_success:
            producer.produce(
                topic="payments",
                key=order["order_id"],
                value=json.dumps(order).encode("utf-8"),
                callback=delivery_report,
            )

            print(
                f"✅ Payment of ₹{total} accepted for order {order['order_id']} by {order['user']}"
            )
        else:
            print(
                f"❌ Payment of ₹{total} FAILED for order {order['order_id']} by {order['user']}"
            )


except KeyboardInterrupt:
    print("\n🔴 Stopping Payment Service")

finally:
    consumer.close()
