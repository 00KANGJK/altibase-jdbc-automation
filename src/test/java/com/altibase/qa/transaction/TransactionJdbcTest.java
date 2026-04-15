package com.altibase.qa.transaction;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_342_001 commit persists pending changes")
    void tc342001Commit() {
        String tableName = DbTestSupport.uniqueName("QA_COMMIT_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.begin(connection);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

        Connection second = jdbc.open();
        try {
            assertThat(jdbc.queryForString(second, "select count(*) from " + tableName)).isEqualTo("0");
            jdbc.commit(connection);
            assertThat(jdbc.queryForString(second, "select count(*) from " + tableName)).isEqualTo("1");
        } finally {
            jdbc.closeQuietly(second);
        }
    }

    @Test
    @DisplayName("TC_343_001 rollback to savepoint restores the transaction to a savepoint")
    void tc343001RollbackToSavepoint() {
        String tableName = DbTestSupport.uniqueName("QA_SAVEPOINT_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.begin(connection);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "savepoint sp1");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
        jdbc.executeUpdate(connection, "rollback to savepoint sp1");
        jdbc.commit(connection);

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select max(c1) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("TC_344_001 lock table in exclusive mode blocks competing locks")
    void tc344001LockTableInExclusiveMode() {
        String tableName = DbTestSupport.uniqueName("QA_LOCK_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        Connection second = jdbc.open();
        try {
            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "lock table " + tableName + " in exclusive mode");

            jdbc.begin(second);
            assertThatThrownBy(() -> jdbc.executeUpdate(second, "lock table " + tableName + " in exclusive mode nowait"))
                    .isInstanceOf(IllegalStateException.class);

            jdbc.rollback(connection);
            jdbc.executeUpdate(second, "lock table " + tableName + " in exclusive mode");
            jdbc.rollback(second);
        } finally {
            jdbc.closeQuietly(second);
        }
    }

    @Test
    @DisplayName("Additional negative case: rollback discards uncommitted changes")
    void rollbackDiscardsChanges() {
        String tableName = DbTestSupport.uniqueName("QA_ROLLBACK_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.begin(connection);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.rollback(connection);

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
    }
}
