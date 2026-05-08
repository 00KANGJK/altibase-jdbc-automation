package com.altibase.qa.dblink;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import com.altibase.qa.support.FeatureProbe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@SuppressWarnings({"SqlNoDataSourceInspection", "SqlSourceToSinkFlow"})
class DatabaseLinkJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional environment case: private database link selects remote rows with REMOTE_TABLE")
    void privateDatabaseLinkSelectsRemoteRowsWithRemoteTable() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DBL");
        String tableName = DbTestSupport.uniqueName("QA_DBL_SRC");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'ALPHA')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'BETA')");
        createPrivateDatabaseLink(linkName);

        var result = jdbc.query(
                connection,
                "select c1, c2 from remote_table(" + linkName +
                        ", 'select c1, c2 from " + tableName + " order by c1')"
        );

        assertThat(result.size()).isEqualTo(2);
        assertThat(result.value(0, "C1").toString()).isEqualTo("1");
        assertThat(result.value(0, "C2")).isEqualTo("ALPHA");
        assertThat(result.value(1, "C1").toString()).isEqualTo("2");
        assertThat(result.value(1, "C2")).isEqualTo("BETA");
    }

    @Test
    @DisplayName("Additional environment case: location descriptor can select a remote table through a database link")
    void locationDescriptorSelectsRemoteTableThroughDatabaseLink() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DBL");
        String tableName = DbTestSupport.uniqueName("QA_DBL_AT");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(7)");
        createPrivateDatabaseLink(linkName);

        String value = jdbc.queryForString(connection, "select c1 from " + tableName + "@" + linkName);

        assertThat(value).isEqualTo("7");
    }

    @Test
    @DisplayName("Additional environment case: REMOTE_EXECUTE_IMMEDIATE runs remote DDL and DML")
    void remoteExecuteImmediateRunsRemoteDdlAndDml() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DBL");
        String tableName = DbTestSupport.uniqueName("QA_DBL_REI");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createPrivateDatabaseLink(linkName);
        remoteExecuteImmediate(linkName, "create table " + tableName + "(c1 integer primary key, c2 varchar(20))");
        remoteExecuteImmediate(linkName, "insert into " + tableName + " values(11, 'REMOTE')");
        remoteExecuteImmediate(linkName, "update " + tableName + " set c2 = 'UPDATED' where c1 = 11");

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 11"))
                .isEqualTo("UPDATED");
    }

    @Test
    @DisplayName("Additional environment case: DB Link metadata views are readable while a private link exists")
    void databaseLinkMetadataViewsAreReadable() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DBL");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));

        createPrivateDatabaseLink(linkName);

        assertThat(jdbc.queryForString(connection, "select count(*) from v$dblink_altilinker_status"))
                .isEqualTo("1");
        assertThat(Integer.parseInt(jdbc.queryForString(connection, "select count(*) from v$dblink_database_link_info")))
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("Additional negative case: REMOTE_TABLE rejects non-SELECT remote statements")
    void remoteTableRejectsNonSelectStatements() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DBL");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        createPrivateDatabaseLink(linkName);

        assertThatThrownBy(() -> jdbc.query(
                connection,
                "select * from remote_table(" + linkName + ", 'create table QA_DBL_BAD(c1 integer)')"
        )).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: REMOTE_EXECUTE_IMMEDIATE reports remote SQL errors")
    void remoteExecuteImmediateReportsRemoteSqlErrors() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DBL");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        createPrivateDatabaseLink(linkName);

        assertThatThrownBy(() -> remoteExecuteImmediate(linkName, "insert into QA_DBL_MISSING values(1)"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: dropped database link cannot be used")
    void droppedDatabaseLinkCannotBeUsed() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DBL");
        createPrivateDatabaseLink(linkName);
        jdbc.executeUpdate(connection, "drop private database link " + linkName);

        assertThatThrownBy(() -> jdbc.query(
                connection,
                "select * from remote_table(" + linkName + ", 'select 1 from dual')"
        )).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: unknown dblink.conf target cannot execute a remote query")
    void unknownTargetCannotExecuteRemoteQuery() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DBL");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));

        Throwable thrown = catchThrowable(() -> {
            createPrivateDatabaseLink(linkName, "QA_MISSING_TARGET");
            jdbc.query(connection, "select * from remote_table(" + linkName + ", 'select 1 from dual')");
        });

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
    }

    private void createPrivateDatabaseLink(String linkName) {
        createPrivateDatabaseLink(linkName, config.databaseLink().targetName());
    }

    private void createPrivateDatabaseLink(String linkName, String targetName) {
        jdbc.executeUpdate(
                connection,
                "create private database link " + linkName +
                        " connect to " + config.databaseLink().remoteUser() +
                        " identified by " + config.databaseLink().remotePassword() +
                        " using " + targetName
        );
    }

    private void remoteExecuteImmediate(String linkName, String remoteSql) {
        try (CallableStatement callableStatement = connection.prepareCall("{call remote_execute_immediate(?, ?)}")) {
            callableStatement.setString(1, linkName);
            callableStatement.setString(2, remoteSql);
            callableStatement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("REMOTE_EXECUTE_IMMEDIATE failed: " + remoteSql, e);
        }
    }
}
