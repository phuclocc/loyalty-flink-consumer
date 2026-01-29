package vn.ghtk.loyalty.flink.model;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Processed check-in result after business logic
 */
public class ProcessedCheckin implements Serializable {
    
    private String eventId;
    private Long userId;
    private LocalDate checkinDate;
    private LocalDateTime eventTimestamp;
    private String yearMonth;
    private int pointsEarned;
    private int checkinOrder;
    private boolean duplicate;
    private String errorMessage;

    public ProcessedCheckin() {
    }

    public ProcessedCheckin(String eventId, Long userId, LocalDate checkinDate,
                           LocalDateTime eventTimestamp, String yearMonth,
                           int pointsEarned, int checkinOrder,
                           boolean duplicate, String errorMessage) {
        this.eventId = eventId;
        this.userId = userId;
        this.checkinDate = checkinDate;
        this.eventTimestamp = eventTimestamp;
        this.yearMonth = yearMonth;
        this.pointsEarned = pointsEarned;
        this.checkinOrder = checkinOrder;
        this.duplicate = duplicate;
        this.errorMessage = errorMessage;
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

    public int getPointsEarned() {
        return pointsEarned;
    }

    public void setPointsEarned(int pointsEarned) {
        this.pointsEarned = pointsEarned;
    }

    public int getCheckinOrder() {
        return checkinOrder;
    }

    public void setCheckinOrder(int checkinOrder) {
        this.checkinOrder = checkinOrder;
    }

    public boolean isDuplicate() {
        return duplicate;
    }

    public void setDuplicate(boolean duplicate) {
        this.duplicate = duplicate;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return "ProcessedCheckin{" +
                "eventId='" + eventId + '\'' +
                ", userId=" + userId +
                ", checkinDate=" + checkinDate +
                ", pointsEarned=" + pointsEarned +
                ", checkinOrder=" + checkinOrder +
                ", duplicate=" + duplicate +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
