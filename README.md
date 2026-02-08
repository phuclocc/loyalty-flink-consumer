# Loyalty Flink Consumer

> **Part of Loyalty System:** Đây là Apache Flink streaming job (Stream Processor) trong hệ thống 3-tier architecture.
> - **loyalty-infra** - Infrastructure (xem [../loyalty-infra](../loyalty-infra))
> - **loyalty-service** - Spring Boot app (xem [../loyalty-service](../loyalty-service))
> - **loyalty-flink-consumer** - Job này

Apache Flink consumer job cho xử lý loyalty check-in events từ Kafka với exactly-once semantics.

## 🚀 Tech Stack

- **Java 17** (target, để chạy trên Flink 1.18.1 Java 17 containers)
- **Apache Flink 1.18.1**
- **Kafka** - Source (`loyalty.checkin.raw`) và Sink (`loyalty.point.transaction`)
- **MySQL 8.0** - Query để dedup và calculate (không ghi trực tiếp)
- **Jackson** - JSON serialization/deserialization

## 📋 Tính năng

### Architecture: Kafka → Flink → Kafka

```
loyalty.checkin.raw (Kafka)
         ↓
    Flink Job
  - Consume events
  - Dedup (state + DB check)
  - Apply business rule (points)
  - Calculate checkin order
         ↓
loyalty.point.transaction (Kafka)
         ↓
  loyalty-service (DB Writer)
         ↓
      MySQL
```

### 1. Kafka Source
- Consumer từ topic `loyalty.checkin.raw`
- Exactly-once semantics với isolation level `read_committed`
- Deduplication theo `eventId` (MapState + DB check)

### 2. Stream Processing
- **KeyBy userId** để process events per user
- **Dedup layer**: MapState (TTL 7 days) + DB check
- **Business logic**: Calculate checkin order, apply points rule
- **Output**: `PointTransactionEvent` với transactionId unique

### 3. Kafka Sink (Transactional)
- Publish `PointTransactionEvent` lên topic `loyalty.point.transaction`
- Kafka transactional producer (`DeliveryGuarantee.EXACTLY_ONCE`)
- Transactional ID prefix: `flink-loyalty-tx-`
- **Key by userId**: Ensures same user events → same partition → prevents deadlock in DB Writer

### 4. Checkpoint & State
- Checkpoint interval: 60 seconds
- State backend: Hashmap (in-memory, rollback version - không dùng RocksDB)
- Checkpoint storage: Local filesystem (`file:///tmp/flink-checkpoints`)
- Savepoint support for upgrades

## 🏗️ Cấu trúc

```
loyalty-flink-consumer/
├── pom.xml
├── README.md
├── src/
│   └── main/
│       ├── java/vn/ghtk/loyalty/flink/
│       │   ├── CheckinEventConsumerJob.java              # Main job (Kafka → Kafka)
│       │   ├── model/
│       │   │   ├── CheckinEvent.java                     # Input event
│       │   │   ├── PointTransactionEvent.java            # Output event
│       │   │   └── EventMetadata.java                    # Metadata
│       │   ├── function/
│       │   │   ├── CheckinEventDeserializer.java         # Kafka deserializer
│       │   │   ├── PointTransactionProcessFunction.java  # Dedup + business logic
│       │   │   └── PointTransactionSerializer.java       # Kafka serializer
│       │   └── config/
│       │       └── JobConfig.java                        # Job configuration
│       └── resources/
│           └── log4j2.properties
└── target/
    └── loyalty-flink-consumer-1.0.0-SNAPSHOT.jar
```

## 🛠️ Build

```powershell
mvn clean package -DskipTests
```

Output: `target/loyalty-flink-consumer-1.0.0-SNAPSHOT.jar`

## 🚀 Deploy to Flink Cluster

### Prerequisites
- Infrastructure đã chạy (xem [../loyalty-infra](../loyalty-infra))
- Kafka topics đã được tạo

### 1. Copy JAR vào Flink JobManager

```powershell
docker cp target\loyalty-flink-consumer-1.0.0-SNAPSHOT.jar loyalty-flink-jobmanager:/opt/flink/
```

### 2. Submit job (1 dòng)

```powershell
docker exec -it loyalty-flink-jobmanager flink run -c vn.ghtk.loyalty.flink.CheckinEventConsumerJob -p 4 /opt/flink/loyalty-flink-consumer-1.0.0-SNAPSHOT.jar --kafka.bootstrap.servers kafka:29092 --kafka.topic loyalty.checkin.raw --kafka.group.id loyalty-flink-consumer --kafka.output.topic loyalty.point.transaction --mysql.url jdbc:mysql://mysql:3306/loyalty_db --mysql.username root --mysql.password root --checkpoint.interval 60000 --parallelism 4
```

### 3. Verify job

- **Flink UI**: http://localhost:8081
- **Kafka UI**: http://localhost:8090
  - Check topic `loyalty.checkin.raw` (input)
  - Check topic `loyalty.point.transaction` (output)

## ⚙️ Configuration

Job arguments (all optional, có defaults):

| Argument | Default | Description |
|----------|---------|-------------|
| `--kafka.bootstrap.servers` | `localhost:9092` | Kafka bootstrap servers |
| `--kafka.topic` | `loyalty.checkin.raw` | Input Kafka topic |
| `--kafka.output.topic` | `loyalty.point.transaction` | Output Kafka topic |
| `--kafka.group.id` | `loyalty-flink-consumer` | Consumer group ID |
| `--mysql.url` | `jdbc:mysql://localhost:3306/loyalty_db` | MySQL JDBC URL (for query only) |
| `--mysql.username` | `root` | MySQL username |
| `--mysql.password` | `root` | MySQL password |
| `--checkpoint.interval` | `60000` | Checkpoint interval (ms) |
| `--parallelism` | `4` | Job parallelism |

## 📊 Sizing & Performance

### Quy mô dự kiến

- **1 triệu event/ngày**: ~12 event/giây
- **10 triệu event/ngày**: ~116 event/giây
- **50 triệu event/ngày**: ~579 event/giây

### Sizing cho 1 triệu event/ngày

- **Kafka Partitions**: 4
- **Flink Parallelism**: 4
- **Checkpoint Interval**: 60 seconds
- **State Size**: ~50 MB (1 triệu event IDs * 50 bytes)
- **Memory per TaskManager**: 2 GB
- **CPU per TaskManager**: 2 cores

### Sizing cho 10 triệu event/ngày

- **Kafka Partitions**: 8
- **Flink Parallelism**: 8
- **Checkpoint Interval**: 60 seconds
- **State Size**: ~500 MB (10 triệu event IDs * 50 bytes)
- **Memory per TaskManager**: 4 GB
- **CPU per TaskManager**: 2 cores

## 🔒 Exactly-Once Guarantees

1. **Kafka → Flink**: Flink Kafka connector với offset commit sau checkpoint
2. **Flink State**: MapState cho deduplication theo eventId (TTL 7 days)
3. **Flink → Kafka**: Kafka transactional sink (`DeliveryGuarantee.EXACTLY_ONCE`)
   - **Partitioning**: Messages keyed by `userId` → same user always in same partition
4. **Kafka → loyalty-service**: Consumer manual commit + DB dedup theo transactionId
   - **Concurrency**: 4 threads (match 4 partitions) → no deadlock vì same userId = same thread

**End-to-end exactly-once**:
- API → Kafka (idempotent producer)
- Kafka → Flink (checkpoint + transactional sink)
- Flink → Kafka (transactional)
- Kafka → DB (manual commit + idempotent write)

## 📝 Notes

- **State TTL**: 7 ngày (event IDs auto-expire)
- **Checkpoint storage**: Local (nếu muốn persist, dùng MinIO/S3)
- **State backend**: Hashmap (in-memory). Nếu cần large state, đổi sang RocksDB.
- **MySQL role**: Chỉ dùng để query checkin count, không ghi trực tiếp.

## 🐛 Troubleshooting

### Job failed with checkpoint timeout
- Tăng `execution.checkpointing.timeout` trong config
- Giảm checkpoint interval nếu state quá lớn

### OutOfMemoryError
- Tăng heap memory cho TaskManager
- Enable RocksDB state backend cho large state

### Cannot deserialize event
- Check event schema compatibility giữa producer và consumer
- Verify Jackson config (LocalDateTime serialization)

### Duplicate transactions in DB
- Check DB Writer deduplication logic
- Verify unique constraint trên `transactionId` hoặc `description`

## 🔄 Operations

### Cancel job
```powershell
docker exec -it loyalty-flink-jobmanager flink list
docker exec -it loyalty-flink-jobmanager flink cancel <JOB_ID>
```

### Cancel with savepoint
```powershell
docker exec -it loyalty-flink-jobmanager flink cancel -s file:///tmp/flink-savepoints <JOB_ID>
```

### Resume from savepoint
```powershell
docker exec -it loyalty-flink-jobmanager flink run -s file:///tmp/flink-savepoints/<savepoint-dir> -c vn.ghtk.loyalty.flink.CheckinEventConsumerJob /opt/flink/loyalty-flink-consumer-1.0.0-SNAPSHOT.jar ...
```

## 📚 References

- [Flink Kafka Connector](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/datastream/kafka/)
- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Flink State & Fault Tolerance](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/concepts/stateful-stream-processing/)

## 📄 License

Internal project
