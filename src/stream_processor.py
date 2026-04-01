import json
import time
from collections import defaultdict

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import CONSUMER_DEFAULTS, PRODUCER_DEFAULTS, SCHEMA_REGISTRY_CLIENT


schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_CLIENT})

with open("../schemas/order_schema_v2.avsc") as f:
    schema_str = f.read()

avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer = Consumer(
    {
        **CONSUMER_DEFAULTS,
        "group.id": "stream-processor",
        "enable.auto.commit": False,
    }
)

producer = Producer({**PRODUCER_DEFAULTS})

consumer.subscribe(["orders"])


user_aggregates = defaultdict(lambda: {"order_count": 0, "total_revenue": 0})
revenue_by_minute = defaultdict(int)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def get_minute_bucket(timestamp):
    return int(timestamp // 60) * 60


print("Stream processor running...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            error_event = {"error": str(msg.error()), "timestamp": time.time()}
            producer.produce(
                topic="error_events",
                value=json.dumps(error_event).encode("utf-8"),
                callback=delivery_report,
            )
            continue

        try:
            order = avro_deserializer(
                msg.value(), SerializationContext("orders", MessageField.VALUE)
            )
        except Exception as e:
            error_event = {
                "error": str(e),
                "raw": str(msg.value()),
                "timestamp": time.time(),
            }
            producer.produce(
                topic="error_events",
                value=json.dumps(error_event).encode("utf-8"),
                callback=delivery_report,
            )
            consumer.commit(message=msg)
            continue

        if order["quantity"] <= 0:
            error_event = {
                "error": "Invalid quantity",
                "order_id": order["order_id"],
                "timestamp": time.time(),
            }
            producer.produce(
                topic="error_events",
                value=json.dumps(error_event).encode("utf-8"),
                callback=delivery_report,
            )
            consumer.commit(message=msg)
            continue

        # --- Compute revenue for this order ---
        revenue = order["price"] * order["quantity"]
        user = order["user"]

        # --- Update user aggregates ---
        user_aggregates[user]["order_count"] += 1
        user_aggregates[user]["total_revenue"] += revenue

        user_agg_event = {
            "user": user,
            "order_count": user_aggregates[user]["order_count"],
            "total_revenue": user_aggregates[user]["total_revenue"],
            "timestamp": time.time(),
        }

        producer.produce(
            topic="user_aggregates",
            key=user,
            value=json.dumps(user_agg_event).encode("utf-8"),
            callback=delivery_report,
        )

        print(
            f"👤 {user} | Orders: {user_agg_event['order_count']} | Revenue: ₹{user_agg_event['total_revenue']}"
        )

        # --- Update revenue per minute ---
        bucket = get_minute_bucket(order["timestamp"])
        revenue_by_minute[bucket] += revenue

        revenue_event = {
            "minute_bucket": bucket,
            "revenue": revenue_by_minute[bucket],
            "timestamp": time.time(),
        }

        producer.produce(
            topic="revenue_metrics",
            key=str(bucket),
            value=json.dumps(revenue_event).encode("utf-8"),
            callback=delivery_report,
        )

        print(f"📊 Minute {bucket} | Running revenue: ₹{revenue_by_minute[bucket]}")

        consumer.commit(message=msg)

except KeyboardInterrupt:
    print("\n🔴 Stopping Stream Processor")

finally:
    consumer.close()
