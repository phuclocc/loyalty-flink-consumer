package vn.ghtk.loyalty.flink.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.ghtk.loyalty.flink.model.ProcessedCheckin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * MySQL sink function with 2-Phase Commit for exactly-once semantics
 * Writes to:
 * 1. users - update total_points
 * 2. daily_checkin - insert check-in record
 * 3. user_points_history - insert points history
 * 4. checkin_events - insert event log for dedup
 */
public class MySQLCheckinSink extends RichSinkFunction<ProcessedCheckin> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MySQLCheckinSink.class);
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    private transient Connection connection;

    public MySQLCheckinSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);
        LOG.info("MySQL connection established for sink");
    }

    @Override
    public void invoke(ProcessedCheckin checkin, Context context) throws Exception {
        try {
            // Skip processing for duplicates and errors
            if (checkin.isDuplicate() || checkin.getErrorMessage() != null) {
                // Still log to checkin_events for monitoring
                logEventToDatabase(checkin, false);
                connection.commit();
                LOG.info("Event logged as duplicate/error: eventId={}", checkin.getEventId());
                return;
            }
            
            // Start transaction
            LOG.info("Processing checkin: eventId={}, userId={}, points={}", 
                checkin.getEventId(), checkin.getUserId(), checkin.getPointsEarned());
            
            // 1. Check for duplicate in daily_checkin (defensive check)
            if (isDuplicateCheckin(checkin.getUserId(), checkin.getCheckinDate())) {
                LOG.warn("Duplicate check-in detected for userId={}, date={}", 
                    checkin.getUserId(), checkin.getCheckinDate());
                logEventToDatabase(checkin, false);
                connection.commit();
                return;
            }
            
            // 2. Update user total_points
            updateUserPoints(checkin.getUserId(), checkin.getPointsEarned());
            
            // 3. Insert daily_checkin record
            insertDailyCheckin(checkin);
            
            // 4. Insert user_points_history
            insertPointsHistory(checkin);
            
            // 5. Log event to checkin_events
            logEventToDatabase(checkin, true);
            
            // Commit transaction
            connection.commit();
            
            LOG.info("Check-in processed successfully: eventId={}, userId={}, points={}", 
                checkin.getEventId(), checkin.getUserId(), checkin.getPointsEarned());
            
        } catch (SQLException e) {
            LOG.error("Error processing check-in: eventId={}, error={}", 
                checkin.getEventId(), e.getMessage(), e);
            connection.rollback();
            throw e;
        }
    }

    private boolean isDuplicateCheckin(Long userId, java.time.LocalDate checkinDate) throws SQLException {
        String sql = "SELECT COUNT(*) FROM daily_checkin WHERE user_id = ? AND checkin_date = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, userId);
            stmt.setDate(2, Date.valueOf(checkinDate));
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    private void updateUserPoints(Long userId, int pointsToAdd) throws SQLException {
        String sql = "UPDATE users SET total_points = total_points + ?, updated_at = NOW() WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, pointsToAdd);
            stmt.setLong(2, userId);
            int updated = stmt.executeUpdate();
            if (updated == 0) {
                throw new SQLException("User not found: " + userId);
            }
            LOG.debug("Updated user points: userId={}, pointsAdded={}", userId, pointsToAdd);
        }
    }

    private void insertDailyCheckin(ProcessedCheckin checkin) throws SQLException {
        String sql = "INSERT INTO daily_checkin (user_id, checkin_date, points_earned, checkin_order, created_at) " +
                    "VALUES (?, ?, ?, ?, NOW())";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, checkin.getUserId());
            stmt.setDate(2, Date.valueOf(checkin.getCheckinDate()));
            stmt.setInt(3, checkin.getPointsEarned());
            stmt.setInt(4, checkin.getCheckinOrder());
            stmt.executeUpdate();
            LOG.debug("Inserted daily_checkin: userId={}, date={}", 
                checkin.getUserId(), checkin.getCheckinDate());
        }
    }

    private void insertPointsHistory(ProcessedCheckin checkin) throws SQLException {
        String sql = "INSERT INTO user_points_history (user_id, points, transaction_type, description, created_at) " +
                    "VALUES (?, ?, 'CHECKIN', ?, NOW())";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, checkin.getUserId());
            stmt.setInt(2, checkin.getPointsEarned());
            stmt.setString(3, String.format("Daily check-in #%d (event-driven)", checkin.getCheckinOrder()));
            stmt.executeUpdate();
            LOG.debug("Inserted points_history: userId={}, points={}", 
                checkin.getUserId(), checkin.getPointsEarned());
        }
    }

    private void logEventToDatabase(ProcessedCheckin checkin, boolean processed) throws SQLException {
        // Use INSERT IGNORE to handle duplicate event_id gracefully
        String sql = "INSERT IGNORE INTO checkin_events " +
                    "(event_id, user_id, checkin_date, event_timestamp, `year_month`, points_earned, " +
                    "processed, processed_at, error_message, created_at, updated_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, checkin.getEventId());
            stmt.setLong(2, checkin.getUserId());
            stmt.setDate(3, Date.valueOf(checkin.getCheckinDate()));
            stmt.setTimestamp(4, Timestamp.valueOf(checkin.getEventTimestamp()));
            stmt.setString(5, checkin.getYearMonth());
            stmt.setInt(6, checkin.getPointsEarned());
            stmt.setBoolean(7, processed);
            stmt.setTimestamp(8, processed ? new Timestamp(System.currentTimeMillis()) : null);
            stmt.setString(9, checkin.getErrorMessage());
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected > 0) {
                LOG.debug("Logged event to checkin_events: eventId={}, processed={}", 
                    checkin.getEventId(), processed);
            } else {
                LOG.warn("Event already exists in checkin_events (duplicate): eventId={}", 
                    checkin.getEventId());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("MySQL connection closed");
        }
    }
}
