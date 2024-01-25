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

Kafka in `confluentinc/cp-zookeeper`:

```bash
[appuser@d940ac41c21b kafka]$ pwd
/etc/kafka
```

---

### Check if port opened:

```bash
$ nc -vz kafka-broker-1 29092
Connection to kafka-broker-1 (172.22.0.4) 29092 port [tcp/*] succeeded!
```

### Check what uses a port:

```bash
$ sudo netstat -anp | grep 8083
```

### Find in child directories

```bash
$ ls -d */ | grep "bla-bla"
$ ls -d */*/ | grep "bla-bla"
$ ls -d */*/*/ | grep "bla-bla"
$ ls -d */*/*/*/ | grep "bla-bla"
etc.
```

### Create topic:

```bash
$ docker exec -it kafka-broker-1 kafka-topics --create --bootstrap-server localhost:29092 --topic kinaction_hw --partitions 3 --replication-factor 3
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic kinaction_hw.
```

### List topics:

```bash
$ docker exec -it kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:29092
kinaction_hw
```

```bash
$ docker exec -it kafka-broker-1 kafka-topics --describe --topic kinaction_hw --bootstrap-server localhost:29092
Topic: kinaction_hw	TopicId: Zjx4vTh4TX6t7woWQkalWQ	PartitionCount: 3	ReplicationFactor: 3	Configs: 
	Topic: kinaction_hw	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: kinaction_hw	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: kinaction_hw	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
```

### Delete topic:

```bash
docker exec -it kafka-broker-1 kafka-topics --delete --topic mysql_topic --bootstrap-server kafka-broker-1:29092
```

> Option zookeeper is deprecated, use --bootstrap-server instead!

### Console producer:

```bash
docker exec -it kafka-broker-1 kafka-console-producer --topic kinaction_hw --bootstrap-server localhost:29092
```

### Console consumer:

```bash
docker exec -it kafka-broker-1 kafka-console-consumer --topic kinaction_hw --bootstrap-server localhost:29092 --from-beginning
```

---

### Start Kafka Connect Producer for text:

```bash
docker exec -it kafka-connect connect-standalone /resources-kafka/connect-standalone.properties /resources-kafka/alert-source.properties
```

### Start Kafka Connect Consumer for text:

```bash
docker exec -it kafka-connect kafka-console-consumer --topic kinaction_alert_connect --bootstrap-server kafka-broker-1:29092 --from-beginning
```

### Start Kafka Connect for text source and destination simultaneously:

```bash
docker exec -it kafka-connect connect-standalone /resources-kafka/connect-standalone.properties /resources-kafka/alert-source.properties /resources-kafka/alert-sink.properties
```

### Start Kafka Connect Producer for MySql:

```bash
docker exec -it kafka-connect connect-standalone /resources-kafka/connect-standalone.properties /resources-kafka/connectors/mysql/mysql-source.properties
```

### Start Kafka Connect for MySql source and destination simultaneously:

```bash
docker exec -it kafka-connect connect-standalone /resources-kafka/connect-standalone.properties /resources-kafka/connectors/mysql/mysql-source.properties /resources-kafka/alert-sink.properties
```
