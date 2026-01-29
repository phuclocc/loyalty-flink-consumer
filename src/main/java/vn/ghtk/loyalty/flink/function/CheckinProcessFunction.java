package vn.ghtk.loyalty.flink.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.ghtk.loyalty.flink.model.CheckinEvent;
import vn.ghtk.loyalty.flink.model.ProcessedCheckin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Process function with deduplication logic
 * Uses MapState to track processed event IDs
 */
public class CheckinProcessFunction extends KeyedProcessFunction<String, CheckinEvent, ProcessedCheckin> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CheckinProcessFunction.class);
    
    private final String mysqlUrl;
    private final String mysqlUsername;
    private final String mysqlPassword;
    private final int[] pointsSequence;
    private final int maxCheckinPerMonth;
    
    private transient MapState<String, Boolean> eventIdState;
    private transient Connection connection;

    public CheckinProcessFunction(String mysqlUrl, String mysqlUsername, String mysqlPassword,
                                  int[] pointsSequence, int maxCheckinPerMonth) {
        this.mysqlUrl = mysqlUrl;
        this.mysqlUsername = mysqlUsername;
        this.mysqlPassword = mysqlPassword;
        this.pointsSequence = pointsSequence;
        this.maxCheckinPerMonth = maxCheckinPerMonth;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // Configure MapState for deduplication with TTL
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, Boolean> descriptor = new MapStateDescriptor<>(
                "event-id-state",
                String.class,
                Boolean.class
        );
        descriptor.enableTimeToLive(ttlConfig);
        
        eventIdState = getRuntimeContext().getMapState(descriptor);
        
        // Initialize MySQL connection
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);
        
        LOG.info("CheckinProcessFunction opened successfully");
    }

    @Override
    public void processElement(CheckinEvent event, Context ctx, Collector<ProcessedCheckin> out) throws Exception {
        String eventId = event.getEventId();
        Long userId = event.getUserId();
        
        LOG.info("Processing event: eventId={}, userId={}", eventId, userId);
        
        // Check for duplicate in state
        if (eventIdState.contains(eventId)) {
            LOG.warn("Duplicate event detected in state: eventId={}", eventId);
            ProcessedCheckin duplicate = createDuplicateResult(event);
            out.collect(duplicate);
            return;
        }
        
        // Check for duplicate in database
        if (isDuplicateInDatabase(eventId)) {
            LOG.warn("Duplicate event detected in database: eventId={}", eventId);
            eventIdState.put(eventId, true);
            ProcessedCheckin duplicate = createDuplicateResult(event);
            out.collect(duplicate);
            return;
        }
        
        // Calculate check-in order and points
        int year = event.getCheckinDate().getYear();
        int month = event.getCheckinDate().getMonthValue();
        int currentMonthCheckins = countMonthlyCheckins(userId, year, month);
        
        // Validate monthly limit
        if (currentMonthCheckins >= maxCheckinPerMonth) {
            LOG.warn("Monthly check-in limit reached: userId={}, current={}, max={}", 
                userId, currentMonthCheckins, maxCheckinPerMonth);
            ProcessedCheckin error = createErrorResult(event, 
                "Maximum " + maxCheckinPerMonth + " check-ins per month reached");
            out.collect(error);
            return;
        }
        
        // Calculate check-in order (1-based)
        int checkinOrder = currentMonthCheckins + 1;
        
        // Calculate points earned
        int pointsEarned = 0;
        if (checkinOrder <= pointsSequence.length) {
            pointsEarned = pointsSequence[checkinOrder - 1];
        } else {
            LOG.warn("Check-in order {} exceeds points sequence length {}", 
                checkinOrder, pointsSequence.length);
            pointsEarned = pointsSequence[pointsSequence.length - 1];
        }
        
        // Mark event as processed in state
        eventIdState.put(eventId, true);
        
        // Create processed result
        ProcessedCheckin result = new ProcessedCheckin(
            eventId,
            userId,
            event.getCheckinDate(),
            event.getEventTimestamp(),
            event.getYearMonth(),
            pointsEarned,
            checkinOrder,
            false,
            null
        );
        
        LOG.info("Event processed successfully: eventId={}, userId={}, points={}, order={}", 
            eventId, userId, pointsEarned, checkinOrder);
        
        out.collect(result);
    }

    private boolean isDuplicateInDatabase(String eventId) throws Exception {
        String sql = "SELECT COUNT(*) FROM checkin_events WHERE event_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, eventId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    private int countMonthlyCheckins(Long userId, int year, int month) throws Exception {
        String sql = "SELECT COUNT(*) FROM daily_checkin " +
                    "WHERE user_id = ? AND YEAR(checkin_date) = ? AND MONTH(checkin_date) = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, userId);
            stmt.setInt(2, year);
            stmt.setInt(3, month);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return 0;
    }

    private ProcessedCheckin createDuplicateResult(CheckinEvent event) {
        return new ProcessedCheckin(
            event.getEventId(),
            event.getUserId(),
            event.getCheckinDate(),
            event.getEventTimestamp(),
            event.getYearMonth(),
            0,
            0,
            true,
            "Duplicate event"
        );
    }

    private ProcessedCheckin createErrorResult(CheckinEvent event, String errorMessage) {
        return new ProcessedCheckin(
            event.getEventId(),
            event.getUserId(),
            event.getCheckinDate(),
            event.getEventTimestamp(),
            event.getYearMonth(),
            0,
            0,
            false,
            errorMessage
        );
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("MySQL connection closed");
        }
    }
}
