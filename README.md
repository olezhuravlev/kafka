### The default Kafka ecosystem ports

| Service                                                | Default Port |
|--------------------------------------------------------|--------------|
| Kafka Clients connections port                         | 9092         |
| Kafka private communication port (Control Plane)       | 9093         |
| "Kafka Connect" service port                           | 8083         | 
| ZooKeeper external client connections port             | 2181         |     
| ZooKeeper peers connections port                       | 2888         |
| ZooKeeper host election port                           | 3888         |
| Kafka Schema Registry connection port                  | 8081         |
| REST Proxy (RESTful interface) to Apache Kafka cluster | 8082         |    
| ksqlDB                                                 | 8088         |

---

### List topics:

```bash
docker exec -it kafka-broker-1 kafka-topics --list --bootstrap-server localhost:29092
```

### Create topic:

```bash
docker exec -it kafka-broker-1 kafka-topics --create --bootstrap-server localhost:29092 --topic kinaction_hw --partitions 3 --replication-factor 3
```

### Delete topic:

```bash
docker exec -it kafka-broker-1 kafka-topics --delete --topic kinaction_hw --bootstrap-server localhost:29092
```
