package vn.ghtk.loyalty.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.ghtk.loyalty.flink.config.JobConfig;
import vn.ghtk.loyalty.flink.function.CheckinEventDeserializer;
import vn.ghtk.loyalty.flink.function.CheckinProcessFunction;
import vn.ghtk.loyalty.flink.function.MySQLCheckinSink;
import vn.ghtk.loyalty.flink.model.CheckinEvent;
import vn.ghtk.loyalty.flink.model.ProcessedCheckin;

/**
 * Main Flink job for consuming loyalty check-in events from Kafka
 * and processing them with exactly-once semantics (checkpoint local, no MinIO).
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

        // Checkpoint local (no RocksDB/MinIO in this rollback)
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints/checkpoints");

        LOG.info("Checkpoint: interval={}ms, mode=EXACTLY_ONCE, storage=file:///tmp/flink-checkpoints/checkpoints",
                config.getCheckpointInterval());

        // Kafka source
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

        DataStream<ProcessedCheckin> processedStream = eventStream
                .keyBy(CheckinEvent::getEventId)
                .process(new CheckinProcessFunction(
                        config.getMysqlUrl(),
                        config.getMysqlUsername(),
                        config.getMysqlPassword(),
                        config.getPointsSequence(),
                        config.getMaxCheckinPerMonth()
                ))
                .name("Process & Dedup Check-in Events");

        processedStream
                .addSink(new MySQLCheckinSink(
                        config.getMysqlUrl(),
                        config.getMysqlUsername(),
                        config.getMysqlPassword()
                ))
                .name("MySQL Sink (2PC)");

        LOG.info("Flink job pipeline configured successfully");
        env.execute("Loyalty Check-in Consumer Job");
    }
}
