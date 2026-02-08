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
import vn.ghtk.loyalty.flink.model.PointTransactionEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

/**
 * Process function: dedup + apply rule, output PointTransactionEvent
 * Không ghi DB, chỉ emit event về Kafka (loyalty.point.transaction)
 */
public class PointTransactionProcessFunction extends KeyedProcessFunction<Long, CheckinEvent, PointTransactionEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PointTransactionProcessFunction.class);

    private final String mysqlUrl;
    private final String mysqlUsername;
    private final String mysqlPassword;
    private final int[] pointsSequence;
    private final int maxCheckinPerMonth;

    private transient MapState<String, Boolean> eventIdState;
    private transient Connection connection;

    public PointTransactionProcessFunction(String mysqlUrl, String mysqlUsername, String mysqlPassword,
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

        // MySQL connection for reading checkin count (không ghi)
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);

        LOG.info("PointTransactionProcessFunction opened successfully");
    }

    @Override
    public void processElement(CheckinEvent event, Context ctx, Collector<PointTransactionEvent> out) throws Exception {
        String eventId = event.getEventId();
        Long userId = event.getUserId();

        LOG.info("Processing event: eventId={}, userId={}", eventId, userId);

        // Dedup: check state
        if (eventIdState.contains(eventId)) {
            LOG.warn("Duplicate event in state: eventId={}", eventId);
            return; // Skip duplicate
        }

        // Dedup: check DB (optional, nếu muốn dedup cross-restart không có checkpoint)
        if (isDuplicateInDatabase(eventId)) {
            LOG.warn("Duplicate event in DB: eventId={}", eventId);
            eventIdState.put(eventId, true);
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
            return; // Skip event nếu vượt limit (có thể publish error event nếu cần)
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

        // Mark as processed in state
        eventIdState.put(eventId, true);

        // Generate transactionId
        String transactionId = String.format("%d_%s_%d_%d",
                userId, event.getYearMonth(), checkinOrder, System.currentTimeMillis());

        // Create PointTransactionEvent
        PointTransactionEvent transaction = new PointTransactionEvent(
                transactionId,
                eventId,
                userId,
                event.getCheckinDate(),
                event.getYearMonth(),
                pointsEarned,
                checkinOrder,
                event.getEventTimestamp(),
                LocalDateTime.now(),
                event.getMetadata()
        );

        LOG.info("Emitting transaction: txId={}, userId={}, points={}, order={}",
                transactionId, userId, pointsEarned, checkinOrder);

        out.collect(transaction);
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

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("MySQL connection closed");
        }
    }
}
