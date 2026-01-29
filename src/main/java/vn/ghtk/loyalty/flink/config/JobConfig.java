package vn.ghtk.loyalty.flink.config;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Job configuration holder
 */
public class JobConfig {
    
    // Kafka configuration
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String kafkaGroupId;
    
    // MySQL configuration
    private final String mysqlUrl;
    private final String mysqlUsername;
    private final String mysqlPassword;
    
    // Flink configuration
    private final long checkpointInterval;
    private final int parallelism;
    
    // Business configuration
    private final int[] pointsSequence;
    private final int maxCheckinPerMonth;

    public JobConfig(ParameterTool params) {
        // Kafka
        this.kafkaBootstrapServers = params.get("kafka.bootstrap.servers", "localhost:9092");
        this.kafkaTopic = params.get("kafka.topic", "loyalty.checkin");
        this.kafkaGroupId = params.get("kafka.group.id", "loyalty-consumer-group");
        
        // MySQL
        this.mysqlUrl = params.get("mysql.url", "jdbc:mysql://localhost:3306/loyalty_db");
        this.mysqlUsername = params.get("mysql.username", "root");
        this.mysqlPassword = params.get("mysql.password", "root");
        
        // Flink
        this.checkpointInterval = params.getLong("checkpoint.interval", 60000L);
        this.parallelism = params.getInt("parallelism", 4);
        
        // Business
        String pointsSeqStr = params.get("points.sequence", "1,2,3,5,8,13,21");
        this.pointsSequence = parsePointsSequence(pointsSeqStr);
        this.maxCheckinPerMonth = params.getInt("max.checkin.per.month", 7);
    }

    private int[] parsePointsSequence(String sequence) {
        String[] parts = sequence.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i].trim());
        }
        return result;
    }

    // Getters
    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getMysqlUrl() {
        return mysqlUrl;
    }

    public String getMysqlUsername() {
        return mysqlUsername;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int[] getPointsSequence() {
        return pointsSequence;
    }

    public int getMaxCheckinPerMonth() {
        return maxCheckinPerMonth;
    }

    @Override
    public String toString() {
        return "JobConfig{" +
                "kafkaBootstrapServers='" + kafkaBootstrapServers + '\'' +
                ", kafkaTopic='" + kafkaTopic + '\'' +
                ", kafkaGroupId='" + kafkaGroupId + '\'' +
                ", mysqlUrl='" + mysqlUrl + '\'' +
                ", checkpointInterval=" + checkpointInterval +
                ", parallelism=" + parallelism +
                ", maxCheckinPerMonth=" + maxCheckinPerMonth +
                '}';
    }
}
