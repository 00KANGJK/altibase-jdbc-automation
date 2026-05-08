package com.altibase.qa.replication;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import com.altibase.qa.support.FeatureProbe;
import com.altibase.qa.support.SqlExceptionSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings({"SqlNoDataSourceInspection", "SameParameterValue"})
class ReplicationJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_287_001 CREATE REPLICATION AS MASTER creates replication metadata")
    void tc287001CreateReplicationAsMaster() {
        String tableName = createReplicatedTable("QA_REPL_MASTER");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_MASTER");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " as master " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        assertReplicationMetadata(replicationName, 1);
    }

    @Test
    @DisplayName("TC_287_002 CREATE REPLICATION AS SLAVE creates replication metadata")
    void tc287002CreateReplicationAsSlave() {
        String tableName = createReplicatedTable("QA_REPL_SLAVE");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_SLAVE");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " as slave " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        assertReplicationMetadata(replicationName, 1);
    }

    @Test
    @DisplayName("TC_289_001 CREATE REPLICATION creates active-standby metadata")
    void tc289001CreateReplication() {
        String tableName = createReplicatedTable("QA_REPL_DEFAULT");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_DEFAULT");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        assertReplicationMetadata(replicationName, 1);
    }

    @Test
    @DisplayName("TC_290_005 ALTER REPLICATION DROP TABLE removes a replicated table from metadata")
    void tc290005AlterReplicationDropTable() {
        String tableOne = createReplicatedTable("QA_REPL_DROP1");
        String tableTwo = createReplicatedTable("QA_REPL_DROP2");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_DROP");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableOne, tableTwo)
        );

        jdbc.executeUpdate(
                connection,
                "alter replication " + replicationName + " drop table " + tableMappingClause(tableTwo)
        );

        assertReplicationMetadata(replicationName, 1);
    }

    @Test
    @DisplayName("TC_290_006 ALTER REPLICATION ADD TABLE adds a replicated table to metadata")
    void tc290006AlterReplicationAddTable() {
        String tableOne = createReplicatedTable("QA_REPL_ADD1");
        String tableTwo = createReplicatedTable("QA_REPL_ADD2");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_ADD");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableOne)
        );

        jdbc.executeUpdate(
                connection,
                "alter replication " + replicationName + " add table " + tableMappingClause(tableTwo)
        );

        assertReplicationMetadata(replicationName, 2);
    }

    @Test
    @DisplayName("TC_291_001 DROP REPLICATION removes replication metadata")
    void tc291001DropReplication() {
        String tableName = createReplicatedTable("QA_REPL_DROP_OBJ");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_DROP_OBJ");

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        assertThat(replicationExists(replicationName)).isTrue();

        jdbc.executeUpdate(connection, "drop replication " + replicationName);

        assertThat(replicationExists(replicationName)).isFalse();
    }

    @Test
    @DisplayName("Additional harness case: self-hosted replication START family reports self-replication handshake errors")
    void selfHostedReplicationStartFamilyReportsHandshakeErrors() {
        String tableName = createReplicatedTable("QA_REPL_SELF");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_SELF");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        assertSelfReplicationHandshakeFailure("alter replication " + replicationName + " sync");
        assertSelfReplicationHandshakeFailure("alter replication " + replicationName + " start");
        assertSelfReplicationHandshakeFailure("alter replication " + replicationName + " quickstart");
        assertReplicationDidNotStart("alter replication " + replicationName + " stop");
    }

    @Test
    @DisplayName("Additional replication case: ALTER REPLICATION ADD HOST records another host entry")
    void alterReplicationAddHostRecordsAnotherHostEntry() {
        String tableName = createReplicatedTable("QA_REPL_HOST");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_HOST");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        jdbc.executeUpdate(
                connection,
                "alter replication " + replicationName + " add host '127.0.0.2', " + replicationPort()
        );

        assertReplicationHostCount(replicationName, 2);
    }

    @Test
    @DisplayName("Additional replication negative case: duplicate replication names are rejected")
    void duplicateReplicationNameIsRejected() {
        String tableName = createReplicatedTable("QA_REPL_DUP");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_DUP");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        assertThatThrownBy(() -> jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        )).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional replication negative case: replicated tables must have a primary key")
    void replicatedTableWithoutPrimaryKeyIsRejected() {
        assumeReplicationFeatureEnabled();

        String tableName = DbTestSupport.uniqueName("QA_REPL_NOPK");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_NOPK");
        registerCleanup(() -> dropReplicationQuietly(replicationName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");

        assertThatThrownBy(() -> jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        )).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional replication negative case: missing replicated table is rejected")
    void missingReplicatedTableIsRejected() {
        assumeReplicationFeatureEnabled();

        String tableName = DbTestSupport.uniqueName("QA_REPL_MISSING");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_MISSING");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        assertThatThrownBy(() -> jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        )).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional replication negative case: ALTER REPLICATION ADD TABLE rejects duplicate table mappings")
    void alterReplicationAddTableRejectsDuplicateTableMapping() {
        String tableName = createReplicatedTable("QA_REPL_DUPT");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_DUPT");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        assertThatThrownBy(() -> jdbc.executeUpdate(
                connection,
                "alter replication " + replicationName + " add table " + tableMappingClause(tableName)
        )).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional replication negative case: ALTER REPLICATION DROP TABLE rejects missing mappings")
    void alterReplicationDropTableRejectsMissingTableMapping() {
        String tableName = createReplicatedTable("QA_REPL_DROP_MISS");
        String missingTable = createReplicatedTable("QA_REPL_NOT_IN_REPL");
        String replicationName = DbTestSupport.uniqueName("QA_REPL_DROP_MISS");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(
                connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName)
        );

        assertThatThrownBy(() -> jdbc.executeUpdate(
                connection,
                "alter replication " + replicationName + " drop table " + tableMappingClause(missingTable)
        )).isInstanceOf(IllegalStateException.class);
    }

    private String createReplicatedTable(String prefix) {
        assumeReplicationFeatureEnabled();

        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key)");
        return tableName;
    }

    private void assumeReplicationFeatureEnabled() {
        FeatureProbe.assumeReplicationAvailable(config, jdbc, connection);
    }

    private String selfHostedWithClause() {
        int port = replicationPort();
        return "with '" + config.replication().remoteHost() + "', " + port;
    }

    private int replicationPort() {
        int port = config.replication().remotePort();
        if (port > 0) {
            return port;
        }

        String configuredPort = jdbc.queryForString(connection,
                "select value1 from v$property where name = 'REPLICATION_PORT_NO'");
        return Integer.parseInt(configuredPort.trim());
    }

    private String replicationTableClause(String... tableNames) {
        return Arrays.stream(tableNames)
                .map(this::tableMappingClause)
                .collect(Collectors.joining(", "));
    }

    private String tableMappingClause(String tableName) {
        String schema = config.db().user().toLowerCase(Locale.ROOT);
        return "from " + schema + "." + tableName + " to " + schema + "." + tableName;
    }

    private void assertReplicationMetadata(String replicationName, int expectedItemCount) {
        var result = jdbc.query(
                connection,
                "select replication_name, host_count, item_count, is_started " +
                        "from system_.sys_replications_ where replication_name = '" + replicationName + "'"
        );

        assertThat(result.size()).isEqualTo(1);
        assertThat(String.valueOf(result.value(0, "REPLICATION_NAME"))).isEqualTo(replicationName);
        assertThat(Integer.parseInt(String.valueOf(result.value(0, "HOST_COUNT")))).isEqualTo(1);
        assertThat(Integer.parseInt(String.valueOf(result.value(0, "ITEM_COUNT")))).isEqualTo(expectedItemCount);
        assertThat(Integer.parseInt(String.valueOf(result.value(0, "IS_STARTED")))).isEqualTo(0);
    }

    private void assertReplicationHostCount(String replicationName, int expectedHostCount) {
        String hostCount = jdbc.queryForString(
                connection,
                "select host_count from system_.sys_replications_ where replication_name = '" + replicationName + "'"
        );
        assertThat(Integer.parseInt(hostCount)).isEqualTo(expectedHostCount);
    }

    private boolean replicationExists(String replicationName) {
        return jdbc.exists(
                connection,
                "select replication_name from system_.sys_replications_ where replication_name = '" + replicationName + "'"
        );
    }

    private void assertSelfReplicationHandshakeFailure(String sql) {
        assertThatThrownBy(() -> jdbc.executeUpdate(connection, sql))
                .isInstanceOf(IllegalStateException.class)
                .satisfies(throwable -> assertThat(SqlExceptionSupport.requireSqlException(throwable).getMessage())
                        .containsIgnoringCase("self replication case"));
    }

    private void assertReplicationDidNotStart(String sql) {
        assertThatThrownBy(() -> jdbc.executeUpdate(connection, sql))
                .isInstanceOf(IllegalStateException.class)
                .satisfies(throwable -> assertThat(SqlExceptionSupport.requireSqlException(throwable).getMessage())
                        .containsIgnoringCase("did not start"));
    }

    private void dropReplicationQuietly(String replicationName) {
        try {
            jdbc.executeUpdate(connection, "drop replication " + replicationName);
        } catch (Exception ignored) {
        }
    }
}
