package com.altibase.qa.optimizer;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StatisticsJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_370_001 gather database statistics updates table statistics")
    void tc370001GatherDatabaseStatistics() {
        String tableName = DbTestSupport.uniqueName("QA_DB_STATS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

        gatherDatabaseStats();

        TableStats tableStats = getTableStats("SYS", tableName);
        assertThat(tableStats.numRows()).isGreaterThanOrEqualTo(1L);
        assertThat(tableStats.numPages()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    @DisplayName("TC_370_003 gather statistics for a specific user's table")
    void tc370003GatherStatisticsForSpecificUserTable() throws Exception {
        String userName = createManagedUser("QA_STATS_USER");
        String tableName = DbTestSupport.uniqueName("QA_USER_STATS_TB");

        grantIfNeeded(userName, "create table");
        withUserConnection(userName, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(2)");
        });

        gatherTableStats(userName, tableName);

        TableStats tableStats = getTableStats(userName, tableName);
        assertThat(tableStats.numRows()).isGreaterThanOrEqualTo(2L);
        assertThat(tableStats.avgRowLength()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    @DisplayName("TC_370_004 gather statistics for a specific user's view")
    void tc370004GatherStatisticsForSpecificUserView() throws Exception {
        String userName = createManagedUser("QA_VIEW_STATS_USER");
        String tableName = DbTestSupport.uniqueName("QA_VIEW_STATS_TB");
        String viewName = DbTestSupport.uniqueName("QA_VIEW_STATS");

        grantIfNeeded(userName, "create table");
        grantIfNeeded(userName, "create view");
        withUserConnection(userName, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1)");
            jdbc.executeUpdate(conn, "create view " + viewName + " as select c1 from " + tableName);
        });

        gatherTableStats(userName, viewName);

        TableStats tableStats = getTableStats(userName, viewName);
        assertThat(tableStats.numRows()).isEqualTo(0L);
        assertThat(tableStats.numPages()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    @DisplayName("Additional negative case: gather_table_stats rejects missing objects")
    void gatherStatisticsForMissingObjectFails() {
        assertThatThrownBy(() -> gatherTableStats("SYS", "MISSING_STATS_OBJECT"))
                .isInstanceOf(IllegalStateException.class);
    }

    private void gatherDatabaseStats() {
        try (CallableStatement callableStatement = connection.prepareCall("{call gather_database_stats()}")) {
            callableStatement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to gather database statistics", e);
        }
    }

    private void gatherTableStats(String schema, String objectName) {
        try (CallableStatement callableStatement = connection.prepareCall("{call gather_table_stats(?, ?)}")) {
            callableStatement.setString(1, schema);
            callableStatement.setString(2, objectName);
            callableStatement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to gather table statistics for " + schema + "." + objectName, e);
        }
    }

    private TableStats getTableStats(String schema, String objectName) {
        try (CallableStatement callableStatement = connection.prepareCall("{call get_table_stats(?,?,?, ?,?,?,?,?)}")) {
            callableStatement.setString(1, schema);
            callableStatement.setString(2, objectName);
            callableStatement.setNull(3, Types.VARCHAR);
            callableStatement.registerOutParameter(4, Types.NUMERIC);
            callableStatement.registerOutParameter(5, Types.NUMERIC);
            callableStatement.registerOutParameter(6, Types.NUMERIC);
            callableStatement.registerOutParameter(7, Types.NUMERIC);
            callableStatement.registerOutParameter(8, Types.DOUBLE);
            callableStatement.execute();
            return new TableStats(
                    callableStatement.getLong(4),
                    callableStatement.getLong(5),
                    callableStatement.getLong(6),
                    callableStatement.getLong(7),
                    callableStatement.getDouble(8)
            );
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query table statistics for " + schema + "." + objectName, e);
        }
    }

    private String createManagedUser(String prefix) {
        String userName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));
        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);
        return userName;
    }

    private void grantIfNeeded(String userName, String privilege) {
        try {
            jdbc.executeUpdate(connection, "grant " + privilege + " to " + userName);
        } catch (IllegalStateException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException sqlException
                    && sqlException.getMessage() != null
                    && sqlException.getMessage().contains("already has privileges")) {
                return;
            }
            throw e;
        }
    }

    private void withUserConnection(String userName, SqlWork work) throws Exception {
        Connection userConnection = jdbc.open(userName, userName);
        try {
            work.run(userConnection);
        } finally {
            jdbc.closeQuietly(userConnection);
        }
    }

    private record TableStats(long numRows, long numPages, long avgRowLength, long cachedPages, double oneRowReadTime) {
    }

    @FunctionalInterface
    private interface SqlWork {
        void run(Connection connection) throws Exception;
    }
}
