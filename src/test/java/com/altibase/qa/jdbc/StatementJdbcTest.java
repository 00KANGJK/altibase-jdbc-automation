package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StatementJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_626_001 Statement close releases the statement")
    void tc626001Close() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.close();

        assertThat(stmt.isClosed()).isTrue();
    }

    @Test
    @DisplayName("TC_627_001 Statement execute runs a query")
    void tc627001Execute() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            assertThat(stmt.execute("select user_name() from dual")).isTrue();
        }
    }

    @Test
    @DisplayName("TC_628_001 Statement executeBatch runs queued SQL statements")
    void tc628001ExecuteBatch() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_STMT_BATCH");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement()) {
            stmt.addBatch("insert into " + tableName + " values (1)");
            stmt.addBatch("insert into " + tableName + " values (2)");
            stmt.addBatch("insert into " + tableName + " values (3)");

            assertThat(stmt.executeBatch()).containsExactly(1, 1, 1);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("3");
    }

    @Test
    @DisplayName("TC_629_001 Statement getConnection returns owning connection")
    void tc629001GetConnection() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            assertThat(stmt.getConnection()).isSameAs(connection);
        }
    }

    @Test
    @DisplayName("TC_630_001 Statement getFetchDirection returns fetch forward")
    void tc630001GetFetchDirection() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            assertThat(stmt.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
        }
    }

    @Test
    @DisplayName("TC_631_001 Statement getFetchSize returns configured fetch size")
    void tc631001GetFetchSize() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.setFetchSize(25);
            assertThat(stmt.getFetchSize()).isEqualTo(25);
        }
    }

    @Test
    @DisplayName("TC_632_001 Statement getMaxRows returns configured max rows")
    void tc632001GetMaxRows() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.setMaxRows(10);
            assertThat(stmt.getMaxRows()).isEqualTo(10);
        }
    }

    @Test
    @DisplayName("TC_633_001 Statement getMoreResults returns whether another result exists")
    void tc633001GetMoreResults() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            assertThat(stmt.execute("select user_name() from dual")).isTrue();
            assertThat(stmt.getMoreResults()).isFalse();
        }
    }

    @Test
    @DisplayName("TC_634_001 Statement getMoreResults(int) handles result closing mode")
    void tc634001GetMoreResultsWithMode() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            assertThat(stmt.execute("select user_name() from dual")).isTrue();
            assertThat(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).isFalse();
        }
    }

    @Test
    @DisplayName("TC_635_001 Statement getQueryTimeout returns configured timeout")
    void tc635001GetQueryTimeout() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.setQueryTimeout(9);
            assertThat(stmt.getQueryTimeout()).isEqualTo(9);
        }
    }

    @Test
    @DisplayName("TC_636_001 Statement getResultSet returns the last result set")
    void tc636001GetResultSet() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            assertThat(stmt.execute("select user_name() from dual")).isTrue();
            try (ResultSet rs = stmt.getResultSet()) {
                assertThat(rs).isNotNull();
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualToIgnoringCase("SYS");
            }
        }
    }

    @Test
    @DisplayName("TC_637_001 Statement getResultSetConcurrency returns configured concurrency")
    void tc637001GetResultSetConcurrency() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            assertThat(stmt.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        }
    }

    @Test
    @DisplayName("TC_638_001 Statement getResultSetHoldability returns configured holdability")
    void tc638001GetResultSetHoldability() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability())) {
            assertThat(stmt.getResultSetHoldability()).isEqualTo(connection.getHoldability());
        }
    }

    @Test
    @DisplayName("TC_639_001 Statement getResultSetType returns configured type")
    void tc639001GetResultSetType() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            assertThat(stmt.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
        }
    }

    @Test
    @DisplayName("TC_640_001 Statement getUpdateCount returns affected rows")
    void tc640001GetUpdateCount() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_STMT_UPD");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement()) {
            assertThat(stmt.execute("insert into " + tableName + " values(1)")).isFalse();
            assertThat(stmt.getUpdateCount()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("TC_641_001 Statement getWarnings returns warning state")
    void tc641001GetWarnings() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("select user_name() from dual");
            assertThat(stmt.getWarnings() == null).isTrue();
        }
    }

    @Test
    @DisplayName("TC_642_001 Statement setFetchSize applies fetch size")
    void tc642001SetFetchSize() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.setFetchSize(50);
            assertThat(stmt.getFetchSize()).isEqualTo(50);
        }
    }

    @Test
    @DisplayName("TC_643_001 Statement setEscapeProcessing updates statement behavior")
    void tc643001SetEscapeProcessing() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.setEscapeProcessing(true);
            assertThat(stmt.execute("select user_id() from dual")).isTrue();
        }
    }

    @Test
    @DisplayName("TC_644_001 Statement setPoolable updates poolable flag")
    void tc644001SetPoolable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.setPoolable(false);
            assertThat(stmt.isPoolable()).isFalse();
        }
    }

    @Test
    @DisplayName("TC_645_001 Statement setQueryTimeout applies timeout")
    void tc645001SetQueryTimeout() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.setQueryTimeout(7);
            assertThat(stmt.getQueryTimeout()).isEqualTo(7);
        }
    }

    @Test
    @DisplayName("TC_646_001 Statement isClosed reports close state")
    void tc646001IsClosed() throws Exception {
        Statement stmt = connection.createStatement();
        assertThat(stmt.isClosed()).isFalse();
        stmt.close();

        assertThat(stmt.isClosed()).isTrue();
    }

    @Test
    @DisplayName("TC_647_001 Statement isPoolable reflects poolable flag")
    void tc647001IsPoolable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.setPoolable(true);
            assertThat(stmt.isPoolable()).isTrue();
        }
    }

    @Test
    @DisplayName("추가 작성 TC_628_NEG_001 Statement executeBatch reports errors for invalid batched DML")
    void additionalTc628Neg001ExecuteBatchFailsOnDuplicatePrimaryKey() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_STMT_BATCH_NEG");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int primary key)");

        try (Statement stmt = connection.createStatement()) {
            stmt.addBatch("insert into " + tableName + " values (1)");
            stmt.addBatch("insert into " + tableName + " values (1)");

            assertThatThrownBy(stmt::executeBatch).isInstanceOf(SQLException.class);
        }
    }
}
