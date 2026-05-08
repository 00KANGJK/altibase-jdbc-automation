package com.altibase.qa.transaction;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Savepoint;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_342_001 commit persists pending changes")
    void tc342001Commit() {
        String tableName = DbTestSupport.uniqueName("QA_COMMIT_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection second = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

            assertThat(jdbc.queryForString(second, "select count(*) from " + tableName)).isEqualTo("0");
            jdbc.commit(connection);
            assertThat(jdbc.queryForString(second, "select count(*) from " + tableName)).isEqualTo("1");
        } finally {
            rollbackQuietly(connection);
            jdbc.closeQuietly(second);
        }
    }

    @Test
    @DisplayName("TC_343_001 rollback to savepoint restores the transaction to a savepoint")
    void tc343001RollbackToSavepoint() {
        String tableName = DbTestSupport.uniqueName("QA_SAVEPOINT_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            jdbc.executeUpdate(connection, "savepoint sp1");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
            jdbc.executeUpdate(connection, "rollback to savepoint sp1");
            jdbc.commit(connection);

            assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
            assertThat(jdbc.queryForString(connection, "select max(c1) from " + tableName)).isEqualTo("1");
        } finally {
            rollbackQuietly(connection);
        }
    }

    @Test
    @DisplayName("TC_344_001 lock table in exclusive mode blocks competing locks")
    void tc344001LockTableInExclusiveMode() {
        String tableName = DbTestSupport.uniqueName("QA_LOCK_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection second = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "lock table " + tableName + " in exclusive mode");

            jdbc.begin(second);
            assertThatThrownBy(() -> jdbc.executeUpdate(second, "lock table " + tableName + " in exclusive mode nowait"))
                    .isInstanceOf(IllegalStateException.class);

            jdbc.rollback(connection);
            jdbc.executeUpdate(second, "lock table " + tableName + " in exclusive mode nowait");
            jdbc.rollback(second);
        } finally {
            rollbackQuietly(connection);
            rollbackQuietly(second);
            jdbc.closeQuietly(second);
        }
    }

    @Test
    @DisplayName("TC_341_001 PLANCOMMIT mode: DDL auto-commits but DML requires explicit commit")
    void tc341001PlanCommitMode() {
        String tableName = DbTestSupport.uniqueName("QA_PLANCOMMIT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        try {
            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");

            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
            jdbc.rollback(connection);

            assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
            assertThat(DbTestSupport.tableExists(connection, "SYS", tableName)).isTrue();
        } finally {
            rollbackQuietly(connection);
        }
    }

    @Test
    @DisplayName("Additional negative case: rollback discards uncommitted changes")
    void rollbackDiscardsChanges() {
        String tableName = DbTestSupport.uniqueName("QA_ROLLBACK_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            jdbc.rollback(connection);

            assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
        } finally {
            rollbackQuietly(connection);
        }
    }

    @Test
    @DisplayName("Additional manual case: READ COMMITTED can be applied to an explicit transaction")
    void readCommittedIsolationCanBeApplied() throws Exception {
        Connection tx = jdbc.open();
        try {
            tx.setAutoCommit(false);
            tx.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            assertThat(tx.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
            assertThat(jdbc.queryForString(tx, "select count(*) from dual")).isEqualTo("1");

            tx.commit();
        } finally {
            rollbackQuietly(tx);
            jdbc.closeQuietly(tx);
        }
    }

    @Test
    @DisplayName("Additional manual case: REPEATABLE READ can be applied to an explicit transaction")
    void repeatableReadIsolationCanBeApplied() throws Exception {
        Connection tx = jdbc.open();
        try {
            tx.setAutoCommit(false);
            tx.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            assertThat(tx.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_REPEATABLE_READ);
            assertThat(jdbc.queryForString(tx, "select count(*) from dual")).isEqualTo("1");

            tx.commit();
        } finally {
            rollbackQuietly(tx);
            jdbc.closeQuietly(tx);
        }
    }

    @Test
    @DisplayName("Additional manual case: SERIALIZABLE can be applied to an explicit transaction")
    void serializableIsolationCanBeApplied() throws Exception {
        Connection tx = jdbc.open();
        try {
            tx.setAutoCommit(false);
            tx.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            assertThat(tx.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_SERIALIZABLE);
            assertThat(jdbc.queryForString(tx, "select count(*) from dual")).isEqualTo("1");

            tx.commit();
        } finally {
            rollbackQuietly(tx);
            jdbc.closeQuietly(tx);
        }
    }

    @Test
    @DisplayName("Additional manual case: SET TRANSACTION READ ONLY rejects DML")
    void setTransactionReadOnlyRejectsDml() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_TX_READONLY");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");

        connection.setAutoCommit(false);
        try {
            jdbc.executeUpdate(connection, "set transaction read only");

            assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)"))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            jdbc.rollback(connection);
            connection.setAutoCommit(true);
        }
    }

    @Test
    @DisplayName("Additional manual case: SET TRANSACTION READ WRITE keeps DML enabled")
    void setTransactionReadWriteAllowsDml() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_TX_READWRITE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");

        connection.setAutoCommit(false);
        try {
            jdbc.executeUpdate(connection, "set transaction read write");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            connection.commit();
        } finally {
            rollbackQuietly(connection);
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional manual case: rolling back to an earlier savepoint removes all later changes")
    void rollbackToEarlierSavepointRemovesLaterChanges() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_TX_MULTI_SP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");

        connection.setAutoCommit(false);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            Savepoint firstSavepoint = connection.setSavepoint("SP1");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
            connection.setSavepoint("SP2");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(3)");

            connection.rollback(firstSavepoint);
            connection.commit();
        } finally {
            rollbackQuietly(connection);
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select max(c1) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional boundary case: uncommitted updates are invisible to another session until commit")
    void uncommittedUpdatesAreInvisibleUntilCommit() {
        String tableName = DbTestSupport.uniqueName("QA_TX_VISIBILITY");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection observer = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "update " + tableName + " set c1 = 2");

            assertThat(jdbc.queryForString(observer, "select c1 from " + tableName)).isEqualTo("1");

            jdbc.commit(connection);

            assertThat(jdbc.queryForString(observer, "select c1 from " + tableName)).isEqualTo("2");
        } finally {
            rollbackQuietly(connection);
            jdbc.closeQuietly(observer);
        }
    }

    @Test
    @DisplayName("Additional boundary case: READ COMMITTED transaction sees newly committed rows from another session")
    void readCommittedTransactionSeesNewlyCommittedRows() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_TX_READ_COMMITTED");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection writer = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");

            jdbc.executeUpdate(writer, "insert into " + tableName + " values(2)");

            assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("2");
            connection.commit();
        } finally {
            rollbackQuietly(connection);
            jdbc.closeQuietly(writer);
        }
    }

    @Test
    @DisplayName("Additional negative case: rollback to a released JDBC savepoint is rejected")
    void rollbackToReleasedSavepointIsRejected() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_TX_RELEASE_SP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");

        connection.setAutoCommit(false);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            Savepoint savepoint = connection.setSavepoint("SP_RELEASED");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");

            connection.releaseSavepoint(savepoint);

            assertThatThrownBy(() -> connection.rollback(savepoint))
                    .isInstanceOf(SQLException.class);

            connection.rollback();
        } finally {
            rollbackQuietly(connection);
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
    }

    private void rollbackQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            if (!connection.getAutoCommit()) {
                connection.rollback();
                connection.setAutoCommit(true);
            }
        } catch (Exception ignored) {
        }
    }
}
