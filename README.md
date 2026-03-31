# Real-Time E-Commerce Event Streaming Platform
**Kafka Intern Project 1**

## 📌 Project Overview
This project demonstrates a scalable Kafka-based pipeline simulating an e-commerce system. It handles order validation and notification services with a focus on partitioning correctness, consumer group scalability, and high availability.

---

## 🛠 Cluster Creation and Configuration
The infrastructure consists of a **2-node Kafka Cluster** running in **KRaft mode**.

* **Topics:** `orders`, `payments`, `shipments`
* **Partitions:** 3 (to allow for parallel processing)
* **Replication Factor:** 2 (to ensure fault tolerance)
### Topic Creation `kafka-topics --create`
![Topic Creation](screenshots/create_topic.png)


### Topic Configuration `kafka-topics --describe`
![Topic Configuration](screenshots/all_topics.png)

> *This screenshots show that all partitions have 2 replicas and are spread across both Broker 1 and Broker 2.*

### Produce and Consume

![Topic Configuration](screenshots/KAFKACLI_produce_consume.png)


---

## 🚀 Data Pipeline Logic

### 1. Producer (Order Generation)
The producer generates JSON events and assigns an `order_id` as the **message key**.
* **Why?** Using a key ensures that all events for a specific order are routed to the **same partition**, maintaining strict temporal ordering for that specific entity.

### 2. Validation Service
Implements a strict schema check and business logic validation:
* Verifies presence of `order_id`, `user`, `item`,`category`, `timestamp`, and `quantity`.
* Ensures `quantity > 0`.
* **Staggered Production:** The producer is throttled (1 msg/sec) to allow for real-time observation of the stream.

---

## 👥 Consumer Group Behavior
One of the core requirements was to demonstrate how Kafka handles multiple consumers in a single group.

### Partition Assignment `kafka-consumer-groups --describe```
By running two instances of `validation.py` simultaneously, Kafka automatically rebalanced the partitions:

![validation.py](screenshots/two_valid.png)

* **Consumer 1:** Assigned Partitions 0 and 1.
* **Consumer 2:** Assigned Partition 2.

![validation.py](screenshots/partition_evidence.png)

> *Note the two distinct Consumer IDs splitting the load.*

---

## 🛡 Fault Tolerance Simulation
To test the resilience of the system, we simulated a broker failure.

1.  **Action:** `docker stop kafka2` while the producer was active.
2.  **Observation:**
    * The `notification` consumers experienced a brief connection warning.
    * Kafka triggered a **Leader Election**.
    * Broker 1 took over leadership for all partitions.
    * **No data was lost**, and processing continued seamlessly.

###Broker Failover

![Kafka Down.py](screenshots/kafka2_down.png)
Showing the fact that only one cluster was up

![Dropped Message](screenshots/dropped_cluster.png)

---

## 💻 How to Run

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Run Consumers (Open multiple terminals)
```bash
python validation.py
python notification.py
```

### 3. Run Producer
```bash
python orders.py
```

### 4. Monitor Groups
```bash
docker exec -it kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --group validation --describe
```

![Monitor](screenshots/descibing_things.png)

---

## 📝 Key Learnings
* **Horizontal Scaling:** Learned how adding consumers to a group increases throughput by sharing partition load.
* **KRaft Quorum:** Configured a controller quorum to manage metadata without Zookeeper.
* **Data Integrity:** Validated that using keys prevents "race conditions" where an order update might be processed before the order creation.

---