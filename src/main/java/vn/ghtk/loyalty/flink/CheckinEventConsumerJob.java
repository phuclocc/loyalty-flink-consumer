package vn.ghtk.loyalty.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.ghtk.loyalty.flink.config.JobConfig;
import vn.ghtk.loyalty.flink.function.CheckinEventDeserializer;
import vn.ghtk.loyalty.flink.function.PointTransactionProcessFunction;
import vn.ghtk.loyalty.flink.function.PointTransactionSerializer;
import vn.ghtk.loyalty.flink.model.CheckinEvent;
import vn.ghtk.loyalty.flink.model.PointTransactionEvent;

/**
 * Main Flink job: Kafka (raw) → Flink (dedup + rule) → Kafka (transaction)
 * loyalty.checkin.raw → Flink → loyalty.point.transaction
 */
public class CheckinEventConsumerJob {

    private static final Logger LOG = LoggerFactory.getLogger(CheckinEventConsumerJob.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final JobConfig config = new JobConfig(params);

        LOG.info("Starting Loyalty Check-in Consumer Job with config: {}", config);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getParallelism());

        // Checkpointing for exactly-once
        env.enableCheckpointing(config.getCheckpointInterval());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
        env.getCheckpointConfig().setCheckpointTimeout(600000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        // Checkpoint local
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints/checkpoints");

        LOG.info("Checkpoint: interval={}ms, mode=EXACTLY_ONCE, storage=file:///tmp/flink-checkpoints/checkpoints",
                config.getCheckpointInterval());

        // Kafka source: loyalty.checkin.raw
        KafkaSource<CheckinEvent> kafkaSource = KafkaSource.<CheckinEvent>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getKafkaTopic())
                .setGroupId(config.getKafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CheckinEventDeserializer())
                .setProperty("enable.auto.commit", "false")
                .setProperty("isolation.level", "read_committed")
                .build();

        DataStream<CheckinEvent> eventStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .name("Kafka Check-in Events");

        // Process: dedup + apply rule
        DataStream<PointTransactionEvent> transactionStream = eventStream
                .keyBy(CheckinEvent::getUserId)
                .process(new PointTransactionProcessFunction(
                        config.getMysqlUrl(),
                        config.getMysqlUsername(),
                        config.getMysqlPassword(),
                        config.getPointsSequence(),
                        config.getMaxCheckinPerMonth()
                ))
                .name("Process & Dedup Check-in Events");

        // Kafka sink: loyalty.point.transaction (transactional)
        // Key by userId to ensure same user events go to same partition → same consumer thread
        String outputTopic = config.getOutputTopic();
        KafkaSink<PointTransactionEvent> kafkaSink = KafkaSink.<PointTransactionEvent>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<PointTransactionEvent>builder()
                                .setTopic(outputTopic)
                                .setKeySerializationSchema(event -> 
                                    String.valueOf(event.getUserId()).getBytes())
                                .setValueSerializationSchema(new PointTransactionSerializer())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink-loyalty-tx-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(15 * 60 * 1000)) // 15 minutes
                .build();

        transactionStream
                .sinkTo(kafkaSink)
                .name("Kafka Sink (Transactional)");

        LOG.info("Flink job pipeline: {} → Flink → {}", config.getKafkaTopic(), outputTopic);
        env.execute("Loyalty Check-in Consumer Job");
    }
}
