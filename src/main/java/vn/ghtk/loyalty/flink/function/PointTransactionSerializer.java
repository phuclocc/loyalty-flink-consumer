package vn.ghtk.loyalty.flink.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import vn.ghtk.loyalty.flink.model.PointTransactionEvent;

/**
 * Serializer for PointTransactionEvent to Kafka
 */
public class PointTransactionSerializer implements SerializationSchema<PointTransactionEvent> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    static {
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
    }

    @Override
    public byte[] serialize(PointTransactionEvent event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize PointTransactionEvent", e);
        }
    }
}
