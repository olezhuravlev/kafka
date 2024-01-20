```bash
docker exec -it kafka-broker-1 kafka-topics --list --bootstrap-server localhost:29092
```

```bash
docker exec -it kafka-broker-1 kafka-topics --create --bootstrap-server localhost:29092 --topic kinaction_hw --partitions 3 --replication-factor 3
```

```bash
docker exec -it kafka-broker-1 kafka-topics --delete --topic kinaction_hw --bootstrap-server localhost:29092
```
