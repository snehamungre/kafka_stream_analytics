import random
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import PRODUCER_DEFAULTS, SCHEMA_REGISTRY_CLIENT
from data import order_data


producer_config = {
    **PRODUCER_DEFAULTS,
}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_CLIENT})

with open("schemas/order_schema_v2.avsc") as f:
    schema_str = f.read()


avro_serializer = AvroSerializer(schema_registry_client, schema_str)

total = random.randint(1, 200)

for i in range(1, total):
    data = order_data.generate_order()

    # Send data using order_id as the KEY
    producer.produce(
        topic="orders",
        key=data["order_id"],
        value=avro_serializer(data, SerializationContext("orders", MessageField.VALUE)),
        callback=delivery_report,
    )

    time.sleep(5)

producer.flush()
