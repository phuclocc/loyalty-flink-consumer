package vn.ghtk.loyalty.flink.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Check-in event model
 */
public class CheckinEvent implements Serializable {
    
    @JsonProperty("eventId")
    private String eventId;
    
    @JsonProperty("userId")
    private Long userId;
    
    @JsonProperty("checkinDate")
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate checkinDate;
    
    @JsonProperty("eventTimestamp")
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime eventTimestamp;
    
    @JsonProperty("yearMonth")
    private String yearMonth;
    
    @JsonProperty("metadata")
    private EventMetadata metadata;

    public CheckinEvent() {
    }

    public CheckinEvent(String eventId, Long userId, LocalDate checkinDate, 
                       LocalDateTime eventTimestamp, String yearMonth, EventMetadata metadata) {
        this.eventId = eventId;
        this.userId = userId;
        this.checkinDate = checkinDate;
        this.eventTimestamp = eventTimestamp;
        this.yearMonth = yearMonth;
        this.metadata = metadata;
    }

    // Getters and Setters
    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public LocalDate getCheckinDate() {
        return checkinDate;
    }

    public void setCheckinDate(LocalDate checkinDate) {
        this.checkinDate = checkinDate;
    }

    public LocalDateTime getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(LocalDateTime eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public EventMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(EventMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "CheckinEvent{" +
                "eventId='" + eventId + '\'' +
                ", userId=" + userId +
                ", checkinDate=" + checkinDate +
                ", eventTimestamp=" + eventTimestamp +
                ", yearMonth='" + yearMonth + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}
