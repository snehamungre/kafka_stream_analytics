from confluent_kafka import Consumer
import json

from config import CONSUMER_DEFAULTS

consumer_config = {
    "group.id": "notification-group",
    **CONSUMER_DEFAULTS,
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
        print(
            f"🔔 Notification sent to {order['user']}:  Your order for {order['quantity']} x {order['item']} as been received!"
        )
except KeyboardInterrupt:
    print("\n🔴 Stopping Notificaion Service")

finally:
    consumer.close()
