package com.altibase.qa.jdbc;

import Altibase.jdbc.driver.AltibaseConnectionPoolDataSource;
import Altibase.jdbc.driver.AltibaseDataSource;
import Altibase.jdbc.driver.AltibasePreparedStatement;
import Altibase.jdbc.driver.AltibaseXADataSource;
import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("SqlNoDataSourceInspection")
class JdbcAdvancedFeatureJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional manual case: scrollable result sets support absolute, relative, first, last, and previous navigation")
    void scrollableResultSetSupportsNavigation() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_SCROLL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(4)");

        try (Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet resultSet = statement.executeQuery("select c1 from " + tableName + " order by c1")) {
            assertThat(resultSet.last()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(4);
            assertThat(resultSet.absolute(2)).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(2);
            assertThat(resultSet.relative(1)).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(3);
            assertThat(resultSet.previous()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(2);
            assertThat(resultSet.first()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("Additional manual case: updatable result sets can update, insert, and delete rows")
    void updatableResultSetCanUpdateInsertAndDeleteRows() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_UPDATABLE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'A')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'B')");

        try (Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet resultSet = statement.executeQuery("select c1, c2 from " + tableName)) {
            assertThat(resultSet.next()).isTrue();
            resultSet.updateString("C2", "AA");
            resultSet.updateRow();

            resultSet.moveToInsertRow();
            resultSet.updateInt("C1", 3);
            resultSet.updateString("C2", "C");
            resultSet.insertRow();
        }

        try (Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet resultSet = statement.executeQuery("select c1, c2 from " + tableName + " where c1 = 2")) {
            assertThat(resultSet.next()).isTrue();
            resultSet.deleteRow();
        }

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1")).isEqualTo("AA");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("2");
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 3")).isEqualTo("C");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName + " where c1 = 2")).isEqualTo("0");
    }

    @Test
    @DisplayName("Additional manual case: HOLD_CURSORS_OVER_COMMIT keeps a result set readable after commit")
    void holdCursorsOverCommitKeepsResultSetReadable() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_HOLD");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");

        int originalHoldability = connection.getHoldability();
        connection.setAutoCommit(false);
        connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        try (Statement statement = connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
             ResultSet resultSet = statement.executeQuery("select c1 from " + tableName + " order by c1")) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);

            connection.commit();

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(2);
        } finally {
            connection.setHoldability(originalHoldability);
            connection.setAutoCommit(true);
        }
    }

    @Test
    @DisplayName("Additional manual case: JDBC escape syntax supports functions and temporal literals")
    void jdbcEscapeSyntaxSupportsFunctionsAndTemporalLiterals() throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "select {fn concat('A','B')} as c1, " +
                             "{d '2025-01-01'} as d1, " +
                             "{t '12:00:00'} as t1, " +
                             "{ts '2025-01-01 12:00:00'} as ts1 from dual")) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString("C1")).isEqualTo("AB");
            assertThat(resultSet.getDate("D1").toString()).isEqualTo("2025-01-01");
            assertThat(resultSet.getTime("T1").toString()).isEqualTo("12:00:00");
            assertThat(resultSet.getTimestamp("TS1").toString()).startsWith("2025-01-01 12:00:00");
        }
    }

    @Test
    @DisplayName("Additional manual case: JDBC outer join escape syntax preserves unmatched left rows")
    void jdbcOuterJoinEscapeSyntaxWorks() throws Exception {
        String leftTable = DbTestSupport.uniqueName("QA_OJ_LEFT");
        String rightTable = DbTestSupport.uniqueName("QA_OJ_RIGHT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, rightTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, leftTable));

        jdbc.executeUpdate(connection, "create table " + leftTable + "(c1 integer primary key)");
        jdbc.executeUpdate(connection, "create table " + rightTable + "(c1 integer primary key, c2 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + leftTable + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + leftTable + " values(2)");
        jdbc.executeUpdate(connection, "insert into " + rightTable + " values(1, 'MATCH')");

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "select l.c1, r.c2 from {oj " + leftTable + " l left outer join " + rightTable + " r on l.c1 = r.c1} order by l.c1")) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);
            assertThat(resultSet.getString(2)).isEqualTo("MATCH");

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(2);
            assertThat(resultSet.getString(2)).isNull();
        }
    }

    @Test
    @DisplayName("Additional manual case: JDBC ESCAPE syntax matches literal wildcard characters")
    void jdbcEscapeClauseMatchesLiteralWildcardCharacters() {
        String tableName = DbTestSupport.uniqueName("QA_ESCAPE_LIKE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values('A_B')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values('AXB')");

        assertThat(jdbc.queryForString(
                connection,
                "select count(*) from " + tableName + " where c1 like '%\\_%' {escape '\\'}"
        )).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional manual case: atomic batch rejects a duplicate row as an all-or-nothing unit")
    void atomicBatchRejectsDuplicateRowsAsOneUnit() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_ATOMIC_BATCH");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key)");

        connection.setAutoCommit(false);
        try (PreparedStatement preparedStatement = connection.prepareStatement("insert into " + tableName + "(c1) values(?)")) {
            AltibasePreparedStatement altibasePreparedStatement = (AltibasePreparedStatement) preparedStatement;
            altibasePreparedStatement.setAtomicBatch(true);

            altibasePreparedStatement.setInt(1, 1);
            altibasePreparedStatement.addBatch();
            altibasePreparedStatement.setInt(1, 1);
            altibasePreparedStatement.addBatch();

            assertThatThrownBy(altibasePreparedStatement::executeBatch)
                    .isInstanceOf(SQLException.class);
            assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
        } finally {
            jdbc.rollback(connection);
            connection.setAutoCommit(true);
        }
    }

    @Test
    @DisplayName("Additional manual case: atomic batch inserts all rows when every entry is valid")
    void atomicBatchInsertsAllRowsWhenEveryEntryIsValid() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_ATOMIC_BATCH_OK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 varchar(20))");

        connection.setAutoCommit(false);
        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("insert into " + tableName + "(c1, c2) values(?, ?)")) {
            AltibasePreparedStatement altibasePreparedStatement = (AltibasePreparedStatement) preparedStatement;
            altibasePreparedStatement.setAtomicBatch(true);

            altibasePreparedStatement.setInt(1, 1);
            altibasePreparedStatement.setString(2, "A");
            altibasePreparedStatement.addBatch();
            altibasePreparedStatement.setInt(1, 2);
            altibasePreparedStatement.setString(2, "B");
            altibasePreparedStatement.addBatch();

            assertThat(altibasePreparedStatement.executeBatch()).hasSize(2);
            connection.commit();
        } finally {
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("2");
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 2")).isEqualTo("B");
    }

    @Test
    @DisplayName("Additional manual case: batch update and delete report affected row counts")
    void batchUpdateAndDeleteReportAffectedRows() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_BATCH_UPD_DEL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 20)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 30)");

        try (Statement statement = connection.createStatement()) {
            statement.addBatch("update " + tableName + " set c2 = c2 + 1 where c1 in (1, 2)");
            statement.addBatch("delete from " + tableName + " where c1 = 3");

            assertThat(statement.executeBatch()).containsExactly(2, 1);
        }

        assertThat(jdbc.queryForString(connection, "select sum(c2) from " + tableName)).isEqualTo("32");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("2");
    }

    @Test
    @DisplayName("Additional manual case: AltibaseDataSource opens a JDBC connection with configured properties")
    void altibaseDataSourceOpensConnection() throws Exception {
        AltibaseDataSource dataSource = new AltibaseDataSource();
        dataSource.setServerName(config.db().host());
        dataSource.setPortNumber(config.db().port());
        dataSource.setDatabaseName(config.db().database());
        dataSource.setUser(config.db().user());
        dataSource.setPassword(config.db().password());

        assertThat(dataSource.getServerName()).isEqualTo(config.db().host());
        assertThat(dataSource.getPortNumber()).isEqualTo(config.db().port());
        assertThat(dataSource.getDatabaseName()).isEqualTo(config.db().database());
        assertThat(dataSource.getUser()).isEqualTo(config.db().user());

        try (Connection dataSourceConnection = dataSource.getConnection()) {
            assertThat(jdbc.queryForString(dataSourceConnection, "select user_name() from dual"))
                    .isEqualToIgnoringCase(config.db().user());
        }
    }

    @Test
    @DisplayName("Additional manual case: AltibaseConnectionPoolDataSource returns pooled connections")
    void connectionPoolDataSourceReturnsPooledConnection() throws Exception {
        AltibaseConnectionPoolDataSource dataSource = new AltibaseConnectionPoolDataSource();
        dataSource.setURL(config.db().jdbcUrl());

        PooledConnection pooledConnection = dataSource.getPooledConnection(config.db().user(), config.db().password());
        try {
            try (Connection pooledLogicalConnection = pooledConnection.getConnection()) {
                assertThat(jdbc.queryForString(pooledLogicalConnection, "select user_name() from dual"))
                        .isEqualToIgnoringCase(config.db().user());
            }
        } finally {
            pooledConnection.close();
        }
    }

    @Test
    @DisplayName("Additional manual case: AltibaseXADataSource returns XA connections")
    void xaDataSourceReturnsXaConnection() throws Exception {
        AltibaseXADataSource dataSource = new AltibaseXADataSource();
        dataSource.setURL(config.db().jdbcUrl());

        XAConnection xaConnection = dataSource.getXAConnection(config.db().user(), config.db().password());
        try {
            assertThat(xaConnection.getXAResource()).isNotNull();
            try (Connection logicalConnection = xaConnection.getConnection()) {
                assertThat(jdbc.queryForString(logicalConnection, "select user_name() from dual"))
                        .isEqualToIgnoringCase(config.db().user());
            }
        } finally {
            xaConnection.close();
        }
    }

    @Test
    @DisplayName("Additional manual case: createBlob and truncate allow bounded BLOB inserts")
    void createBlobAndTruncateWorks() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_CREATE_BLOB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 blob)");
        boolean originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            Blob blob = connection.createBlob();
            blob.setBytes(1, new byte[]{0x01, 0x02, 0x03, 0x04});
            blob.truncate(2);

            try (PreparedStatement preparedStatement = connection.prepareStatement("insert into " + tableName + "(c1, c2) values(?, ?)")) {
                preparedStatement.setInt(1, 1);
                preparedStatement.setBlob(2, blob);
                preparedStatement.executeUpdate();
            }
            connection.commit();

            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("select c2 from " + tableName + " where c1 = 1")) {
                assertThat(resultSet.next()).isTrue();
                Blob stored = resultSet.getBlob(1);
                assertThat(stored.length()).isEqualTo(2);
                assertThat(stored.getBytes(1, 2)).containsExactly((byte) 0x01, (byte) 0x02);
            }
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    @Test
    @DisplayName("Additional manual case: createClob supports setString, getSubString, and truncate")
    void createClobSetStringGetSubStringAndTruncateWorks() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_CREATE_CLOB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 clob)");
        boolean originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            Clob clob = connection.createClob();
            clob.setString(1, "altibase");
            clob.truncate(7);

            try (PreparedStatement preparedStatement = connection.prepareStatement("insert into " + tableName + "(c1, c2) values(?, ?)")) {
                preparedStatement.setInt(1, 1);
                preparedStatement.setClob(2, clob);
                preparedStatement.executeUpdate();
            }
            connection.commit();

            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("select c2 from " + tableName + " where c1 = 1")) {
                assertThat(resultSet.next()).isTrue();
                Clob stored = resultSet.getClob(1);
                assertThat(stored.length()).isEqualTo(7);
                assertThat(stored.getSubString(1, 7)).isEqualTo("altibas");
                assertThat(stored.getSubString(4, 3)).isEqualTo("iba");
            }
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }
}
