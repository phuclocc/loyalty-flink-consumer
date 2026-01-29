package vn.ghtk.loyalty.flink.function;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import vn.ghtk.loyalty.flink.model.CheckinEvent;

import java.io.IOException;

/**
 * Kafka deserializer for CheckinEvent
 */
public class CheckinEventDeserializer implements DeserializationSchema<CheckinEvent> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false);

    @Override
    public CheckinEvent deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, CheckinEvent.class);
    }

    @Override
    public boolean isEndOfStream(CheckinEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CheckinEvent> getProducedType() {
        return TypeInformation.of(CheckinEvent.class);
    }
}
