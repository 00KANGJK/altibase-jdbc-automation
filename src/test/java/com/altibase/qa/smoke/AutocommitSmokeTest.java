package com.altibase.qa.smoke;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AutocommitSmokeTest extends BaseDbTest {

    @Test
    @DisplayName("TC_340_001 JDBC connections start in auto-commit mode")
    void tc340001AutoCommitDefaultsToTrue() throws Exception {
        assertThat(connection.getAutoCommit()).isTrue();
    }

    @Test
    @DisplayName("Additional negative case: rollback discards DML when auto-commit is disabled")
    void rollbackDiscardsPendingChanges() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_TX_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.begin(connection);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.rollback(connection);

        assertThat(connection.getAutoCommit()).isFalse();
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
    }
}
