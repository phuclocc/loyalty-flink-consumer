package vn.ghtk.loyalty.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Point transaction event - gửi từ Flink về loyalty-service qua Kafka
 */
public class PointTransactionEvent implements Serializable {
    
    @JsonProperty("transactionId")
    private String transactionId;
    
    @JsonProperty("eventId")
    private String eventId;
    
    @JsonProperty("userId")
    private Long userId;
    
    @JsonProperty("checkinDate")
    private LocalDate checkinDate;
    
    @JsonProperty("yearMonth")
    private String yearMonth;
    
    @JsonProperty("pointsEarned")
    private Integer pointsEarned;
    
    @JsonProperty("checkinOrder")
    private Integer checkinOrder;
    
    @JsonProperty("eventTimestamp")
    private LocalDateTime eventTimestamp;
    
    @JsonProperty("processedAt")
    private LocalDateTime processedAt;
    
    @JsonProperty("metadata")
    private EventMetadata metadata;

    public PointTransactionEvent() {
    }

    public PointTransactionEvent(String transactionId, String eventId, Long userId,
                                 LocalDate checkinDate, String yearMonth,
                                 Integer pointsEarned, Integer checkinOrder,
                                 LocalDateTime eventTimestamp, LocalDateTime processedAt,
                                 EventMetadata metadata) {
        this.transactionId = transactionId;
        this.eventId = eventId;
        this.userId = userId;
        this.checkinDate = checkinDate;
        this.yearMonth = yearMonth;
        this.pointsEarned = pointsEarned;
        this.checkinOrder = checkinOrder;
        this.eventTimestamp = eventTimestamp;
        this.processedAt = processedAt;
        this.metadata = metadata;
    }

    // Getters and Setters
    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

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

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public Integer getPointsEarned() {
        return pointsEarned;
    }

    public void setPointsEarned(Integer pointsEarned) {
        this.pointsEarned = pointsEarned;
    }

    public Integer getCheckinOrder() {
        return checkinOrder;
    }

    public void setCheckinOrder(Integer checkinOrder) {
        this.checkinOrder = checkinOrder;
    }

    public LocalDateTime getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(LocalDateTime eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }

    public EventMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(EventMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "PointTransactionEvent{" +
                "transactionId='" + transactionId + '\'' +
                ", userId=" + userId +
                ", pointsEarned=" + pointsEarned +
                ", checkinDate=" + checkinDate +
                '}';
    }
}
