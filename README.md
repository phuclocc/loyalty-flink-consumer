# Loyalty Flink Consumer

Apache Flink consumer job cho xử lý loyalty check-in events từ Kafka với exactly-once semantics.

## 🚀 Tech Stack

- **Java 21**
- **Apache Flink 1.18.1**
- **Kafka** - Source (loyalty.checkin topic)
- **MySQL 8.0** - Sink (exactly-once with 2PC)
- **Jackson** - JSON serialization/deserialization

## 📋 Tính năng

1. **Kafka Source**
   - Consumer từ topic `loyalty.checkin`
   - Exactly-once semantics
   - Deduplication theo `eventId`

2. **Stream Processing**
   - Validate business rules
   - Calculate points based on monthly check-in order
   - Dedup với MapState (keyed by eventId)

3. **MySQL Sink**
   - 2-Phase Commit (2PC) for exactly-once
   - Atomic writes to 3 tables:
     - `users` - cập nhật total_points
     - `daily_checkin` - log check-in record
     - `user_points_history` - log points transaction
     - `checkin_events` - log event for dedup and monitoring

4. **Checkpoint & State**
   - Checkpoint interval: 60 seconds
   - State backend: RocksDB (for large state)
   - Savepoint support for upgrades

## 🏗️ Cấu trúc

```
loyalty-flink-consumer/
├── pom.xml
├── README.md
├── src/
│   └── main/
│       ├── java/vn/ghtk/loyalty/flink/
│       │   ├── CheckinEventConsumerJob.java        # Main job
│       │   ├── model/
│       │   │   ├── CheckinEvent.java               # Event model
│       │   │   └── EventMetadata.java              # Metadata model
│       │   ├── function/
│       │   │   ├── CheckinEventDeserializer.java   # Kafka deserializer
│       │   │   ├── CheckinProcessFunction.java     # Process function with dedup
│       │   │   └── MySQLCheckinSink.java           # JDBC sink with 2PC
│       │   └── config/
│       │       └── JobConfig.java                  # Job configuration
│       └── resources/
│           └── log4j2.properties
└── target/
    └── loyalty-flink-consumer-1.0.0-SNAPSHOT.jar
```

## 🛠️ Build

```bash
mvn clean package
```

Output: `target/loyalty-flink-consumer-1.0.0-SNAPSHOT.jar`

## 🚀 Run

### Local Development

```bash
# Run từ IDE hoặc Maven
mvn exec:java -Dexec.mainClass="vn.ghtk.loyalty.flink.CheckinEventConsumerJob"
```

### Flink Cluster

```bash
# Submit job to Flink cluster
flink run \
  -c vn.ghtk.loyalty.flink.CheckinEventConsumerJob \
  -p 4 \
  target/loyalty-flink-consumer-1.0.0-SNAPSHOT.jar \
  --kafka.bootstrap.servers localhost:9092 \
  --kafka.topic loyalty.checkin \
  --kafka.group.id loyalty-consumer-group \
  --mysql.url jdbc:mysql://localhost:3306/loyalty_db \
  --mysql.username root \
  --mysql.password root \
  --checkpoint.interval 60000 \
  --parallelism 4
```

## ⚙️ Configuration

Job arguments (all optional, có defaults):

| Argument | Default | Description |
|----------|---------|-------------|
| `--kafka.bootstrap.servers` | `localhost:9092` | Kafka bootstrap servers |
| `--kafka.topic` | `loyalty.checkin` | Kafka topic name |
| `--kafka.group.id` | `loyalty-consumer-group` | Consumer group ID |
| `--mysql.url` | `jdbc:mysql://localhost:3306/loyalty_db` | MySQL JDBC URL |
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

### Sizing cho 50 triệu event/ngày

- **Kafka Partitions**: 16
- **Flink Parallelism**: 16
- **Checkpoint Interval**: 60 seconds
- **State Size**: ~2.5 GB (50 triệu event IDs * 50 bytes)
- **Memory per TaskManager**: 8 GB
- **CPU per TaskManager**: 4 cores

## 🔒 Exactly-Once Guarantees

1. **Kafka → Flink**: Flink Kafka connector với offset commit sau checkpoint
2. **Flink State**: MapState cho deduplication theo eventId
3. **Flink → MySQL**: JDBC sink với 2-Phase Commit (XA transactions)
4. **Checkpoint**: RocksDB state backend với incremental checkpoint

## 📝 Notes

- State TTL: 7 ngày (event IDs auto-expire sau 7 ngày)
- Retry strategy: 3 retries với exponential backoff
- Timeout: 30 seconds per transaction
- Idempotency: Dựa trên unique constraint của `event_id` trong DB

## 🐛 Troubleshooting

### Job failed with checkpoint timeout
- Tăng `execution.checkpointing.timeout` trong config
- Giảm checkpoint interval nếu state quá lớn

### OutOfMemoryError
- Tăng heap memory cho TaskManager
- Enable RocksDB state backend cho large state

### MySQL deadlock
- Kiểm tra index trên `event_id`, `user_id`, `checkin_date`
- Tăng MySQL timeout settings

## 📄 License

Internal project
