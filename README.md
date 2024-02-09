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

### Find directory in child directories

```bash
$ ls -d */ | grep "bla-bla"
$ ls -d */*/ | grep "bla-bla"
$ ls -d */*/*/ | grep "bla-bla"
$ ls -d */*/*/*/ | grep "bla-bla"
etc.
```

### Remove just one container of Docker Compose:
```bash
$ docker-compose rm -s -v -f schema-registry
Going to remove schema-registry
Removing schema-registry ... done
```

### Create topic:

```bash
$ docker exec -it kafka-broker-1 kafka-topics --create --bootstrap-server kafka-broker-1:29092 --topic kinaction_hw --partitions 3 --replication-factor 3
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic kinaction_hw.
```

### List topics:

```bash
$ docker exec -it kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:29092
kinaction_hw
```

```bash
$ docker exec -it kafka-broker-1 kafka-topics --describe --topic kinaction_hw --bootstrap-server kafka-broker-1:29092
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

---

### Create, describe and delete topic:
````bash
$ docker exec -it kafka-broker-1 kafka-topics --create --bootstrap-server kafka-broker-1:29092 --topic kinaction_topicandpart --partitions 2 --replication-factor 2
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic kinaction_topicandpart.

$ docker exec -it kafka-broker-1 kafka-topics --describe --bootstrap-server kafka-broker-1:29092 --topic kinaction_topicandpart   
Topic: kinaction_topicandpart	TopicId: 3QLxjocpT2qFtbmB-4V_iw	PartitionCount: 2	ReplicationFactor: 2	Configs: 
	Topic: kinaction_topicandpart	Partition: 0	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: kinaction_topicandpart	Partition: 1	Leader: 2	Replicas: 2,3	Isr: 2,3

$ docker exec -it kafka-broker-1 kafka-topics --delete --bootstrap-server kafka-broker-1:29092 --topic kinaction_topicandpart

$ docker exec -it kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:29092
__consumer_offsets
_schemas
````

---

### Console producer:

```bash
docker exec -it kafka-broker-1 kafka-console-producer --topic kinaction_hw --bootstrap-server kafka-broker-1:29092
```

### Console consumer:

```bash
docker exec -it kafka-broker-1 kafka-console-consumer --topic kinaction_hw --bootstrap-server kafka-broker-1:29092 --from-beginning
```

---

### Start Kafka Connect Producer for text:

```bash
docker exec -it kafka-connect connect-standalone /resources-kafka/connectors/file/connect-standalone.properties /resources-kafka/connectors/file/alert-source.properties
```

### Start Kafka Connect console Consumer for text:

```bash
docker exec -it kafka-connect kafka-console-consumer --topic kinaction_alert_connect --bootstrap-server kafka-broker-1:29092 --from-beginning
```

### Start Kafka Connect for text source and destination simultaneously:

```bash
docker exec -it kafka-connect connect-standalone /resources-kafka/connectors/file/connect-standalone.properties /resources-kafka/connectors/file/alert-source.properties /resources-kafka/connectors/file/alert-sink.properties
```

### Start Kafka Connect Producer for MySql:

```bash
docker exec -it kafka-connect connect-standalone /resources-kafka/connectors/mysql/connect-standalone.properties /resources-kafka/connectors/mysql/mysql-source.properties
```

### Start Kafka Connect for MySql source and destination simultaneously:

```bash
docker exec -it kafka-connect connect-standalone /resources-kafka/connectors/mysql/connect-standalone.properties /resources-kafka/connectors/mysql/mysql-source.properties /resources-kafka/connectors/mysql/mysql-sink.properties
```

### Launch `zookeeper-shell` utility

````bash
[appuser@711e06753d11 /]$ zookeeper-shell kafka-broker-1:29092
Connecting to kafka-broker-1:29092
Welcome to ZooKeeper!
JLine support is disabled
````

````bash
[appuser@zookeeper-1 /]$ zookeeper-shell zookeeper-1:2181
Connecting to zookeeper-1:2181
Welcome to ZooKeeper!
JLine support is disabled

WATCHER::

WatchedEvent state:SyncConnected type:None path:null
````

````bash
$ docker exec -it kafka-broker-1 zookeeper-shell kafka-broker-1:29092
Connecting to kafka-broker-1:29092
Welcome to ZooKeeper!
JLine support is disabled
````

### Using `zookeeper-shell` utility
 
````bash
$ docker exec -it kafka-broker-1 zookeeper-shell zookeeper-1:2181
Connecting to zookeeper-1:2181
Welcome to ZooKeeper!
JLine support is disabled

WATCHER::

WatchedEvent state:SyncConnected type:None path:null

version
ZooKeeper CLI version: 3.8.3-6ad6d364c7c0bcf0de452d54ebefa3058098ab56, built on 2023-10-05 10:34 UTC

# List of brokers:
ls /brokers/topics
[__consumer_offsets, _schemas]

# Active (current) controller:
get /controller
{"version":2,"brokerid":3,"timestamp":"1707459415021","kraftControllerEpoch":-1}

````

### Create one replica topic:
````bash
$ docker exec -it kafka-broker-1 kafka-topics --create --bootstrap-server kafka-broker-1:29092 --topic kinaction_one_replica
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic kinaction_one_replica.
````

### Get description of a topic:
````bash
$ docker exec -it kafka-broker-1 kafka-topics --describe --bootstrap-server kafka-broker-1:29092 --topic kinaction_one_replica
Topic: kinaction_one_replica	TopicId: sQujtBnrSK20cSS3yFWMrQ	PartitionCount: 1	ReplicationFactor: 1	Configs: 
	Topic: kinaction_one_replica	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
````

### Topic 4 partitions 3 replicas on 3 brokers:
![topic_4part_3repl.png](topic_4part_3repl.png)

> Leader replicas marked with green.

### Topic 4 partitions 3 replicas, broker #3 down (new leader replicas elected):
![topic_4parts_3repl_one_down.png](topic_4parts_3repl_one_down.png)

### Show partitions not finished with replications
````bash
$ docker exec -it kafka-broker-1 kafka-topics --describe --bootstrap-server kafka-broker-1:29092 --under-replicated-partitions
````

### Partitions logs:
````bash
[appuser@711e06753d11 /]$ cat /etc/kafka/server.properties  | grep log.dirs
log.dirs=/var/lib/kafka

[appuser@711e06753d11 /]$ ls -la /var/lib/kafka/data
total 236
drwxrwxr-x 54 appuser root    4096 Feb  9 15:11 .
drwxrwx---  3 appuser root    4096 Dec 20 23:51 ..
-rw-r--r--  1 appuser appuser    0 Feb  9 06:16 .lock
drwxr-xr-x  2 appuser appuser 4096 Feb  9 06:17 __consumer_offsets-0
drwxr-xr-x  2 appuser appuser 4096 Feb  9 08:56 _schemas-0
-rw-r--r--  1 appuser appuser    4 Feb  9 14:53 cleaner-offset-checkpoint
drwxr-xr-x  2 appuser appuser 4096 Feb  9 15:08 kinaction_topicandpart-0
-rw-r--r--  1 appuser appuser    4 Feb  9 15:11 log-start-offset-checkpoint
-rw-r--r--  1 appuser appuser   88 Feb  9 07:25 meta.properties
-rw-r--r--  1 appuser appuser 1235 Feb  9 15:11 recovery-point-offset-checkpoint
-rw-r--r--  1 appuser appuser 1235 Feb  9 15:11 replication-offset-checkpoint

[appuser@711e06753d11 /]$ cd /var/lib/kafka/data/kinaction_topicandpart-0/

[appuser@711e06753d11 kinaction_topicandpart-0]$ ls -la
total 12
drwxr-xr-x  2 appuser appuser     4096 Feb  9 15:08 .
drwxrwxr-x 54 appuser root        4096 Feb  9 15:12 ..
-rw-r--r--  1 appuser appuser 10485760 Feb  9 15:08 00000000000000000000.index
-rw-r--r--  1 appuser appuser        0 Feb  9 15:08 00000000000000000000.log
-rw-r--r--  1 appuser appuser 10485756 Feb  9 15:08 00000000000000000000.timeindex
-rw-r--r--  1 appuser appuser        0 Feb  9 15:08 leader-epoch-checkpoint
-rw-r--r--  1 appuser appuser       43 Feb  9 15:08 partition.metadata
````


### View partition segment:
````bash
$ docker exec -it kafka-broker-1 kafka-dump-log --print-data-log --files /var/lib/kafka/data/kinaction_topicandpart-0/00000000000000000000.log     
Dumping /var/lib/kafka/data/kinaction_topicandpart-0/00000000000000000000.log
Log starting offset: 0

$ docker exec -it kafka-broker-1 kafka-dump-log --print-data-log --files /var/lib/kafka/data/kinaction_topicandpart-0/00000000000000000000.log | awk -F: '{print $NF}' | grep kinaction
Dumping /var/lib/kafka/data/kinaction_topicandpart-0/00000000000000000000.log
````

> `--files` specifies segment file.

### Outside container:
````bash
$ docker exec -it kafka-broker-1 kafka-dump-log --print-data-log --files /var/lib/kafka/data/_schemas-0/00000000000000000000.log
Dumping /var/lib/kafka/data/_schemas-0/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 0 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 0 CreateTime: 1707459421248 size: 96 magic: 2 compresscodec: none crc: 4183771336 isvalid: true
| offset: 0 CreateTime: 1707459421248 keySize: 28 valueSize: -1 sequence: -1 headerKeys: [] key: {"keytype":"NOOP","magic":0}
baseOffset: 1 lastOffset: 1 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 96 CreateTime: 1707459427725 size: 96 magic: 2 compresscodec: none crc: 64730167 isvalid: true
| offset: 1 CreateTime: 1707459427725 keySize: 28 valueSize: -1 sequence: -1 headerKeys: [] key: {"keytype":"NOOP","magic":0}

$ docker exec -it kafka-broker-1 kafka-dump-log --print-data-log --files /var/lib/kafka/data/_schemas-0/00000000000000000000.log | awk -F: '{print $NF}'                 
Dumping /var/lib/kafka/data/_schemas-0/00000000000000000000.log
 0
 true
0}
 true
0}
````

### Within a container placeholder `*` can be used:
````bash
[appuser@711e06753d11 ~]$ kafka-dump-log --print-data-log --files /var/lib/kafka/data/_schemas-0/*.log
Dumping /var/lib/kafka/data/_schemas-0/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 0 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 0 CreateTime: 1707459421248 size: 96 magic: 2 compresscodec: none crc: 4183771336 isvalid: true
| offset: 0 CreateTime: 1707459421248 keySize: 28 valueSize: -1 sequence: -1 headerKeys: [] key: {"keytype":"NOOP","magic":0}
baseOffset: 1 lastOffset: 1 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 96 CreateTime: 1707459427725 size: 96 magic: 2 compresscodec: none crc: 64730167 isvalid: true
| offset: 1 CreateTime: 1707459427725 keySize: 28 valueSize: -1 sequence: -1 headerKeys: [] key: {"keytype":"NOOP","magic":0}

[appuser@711e06753d11 ~]$ kafka-dump-log --print-data-log --files /var/lib/kafka/data/_schemas-0/*.log | awk -F: '{print $NF}' 
Dumping /var/lib/kafka/data/_schemas-0/00000000000000000000.log
 0
 true
0}
 true
0}
````


### Create compact topic:
````bash
$ docker exec -it kafka-broker-1 kafka-topics --create --bootstrap-server kafka-broker-1:29092 --topic kinaction_compact --partitions 3 --replication-factor 3 --config cleanup.policy=compact
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic kinaction_compact.
````

````bash
````

````bash
````

````bash
````
