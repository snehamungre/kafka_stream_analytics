BOOTSTRAP_SERVERS = "localhost:9092"

CONSUMER_DEFAULTS = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "auto.offset.reset": "earliest",
}

PRODUCER_DEFAULTS = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
}
