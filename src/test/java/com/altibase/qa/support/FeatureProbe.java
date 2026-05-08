package com.altibase.qa.support;

import com.altibase.qa.config.TestConfig;
import com.altibase.qa.infra.jdbc.JdbcHelper;
import org.junit.jupiter.api.Assumptions;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;

@SuppressWarnings("SqlNoDataSourceInspection")
public final class FeatureProbe {
    private FeatureProbe() {
    }

    public static void assumeCliUtilitiesEnabled(TestConfig config) {
        assumeEnabled(
                config.execution().enableCliTests() && config.features().cliUtilities(),
                "cliUtilities",
                "execution.enableCliTests and features.cliUtilities must both be true; client/server CLI paths must be executable from the test runner host"
        );
    }

    public static void assumeRecoveryEnabled(TestConfig config) {
        assumeEnabled(
                config.execution().enableRecoveryTests() && config.features().backupRecovery(),
                "backupRecovery",
                "execution.enableRecoveryTests and features.backupRecovery must both be true; this suite may execute recovery/backup-related SQL"
        );
    }

    public static void assumeReplicationSuiteEnabled(TestConfig config) {
        assumeEnabled(
                config.execution().enableReplicationTests() && config.features().replication(),
                "replication",
                "execution.enableReplicationTests and features.replication must both be true"
        );
    }

    public static void assumeReplicationAvailable(TestConfig config, JdbcHelper jdbc, Connection connection) {
        assumeReplicationSuiteEnabled(config);

        String port = jdbc.queryForString(connection,
                "select value1 from v$property where name = 'REPLICATION_PORT_NO'");
        assumeEnabled(
                port != null && !port.isBlank() && !"0".equals(port.trim()),
                "replication",
                "Altibase server property REPLICATION_PORT_NO is 0; enable replication on this server before running replication tests"
        );
    }

    public static void assumeDatabaseLinkAvailable(TestConfig config, JdbcHelper jdbc, Connection connection) {
        assumeEnabled(
                config.features().databaseLink(),
                "databaseLink",
                "features.databaseLink must be true; DBLINK_ENABLE, AltiLinker, and dblink.conf TARGETS must be configured"
        );

        String enabled = jdbc.queryForString(connection,
                "select value1 from v$property where name = 'DBLINK_ENABLE'");
        assumeEnabled(
                "1".equals(enabled == null ? "" : enabled.trim()),
                "databaseLink",
                "Altibase server property DBLINK_ENABLE is not 1; set DBLINK_ENABLE=1 and restart the server"
        );

        try {
            String status = jdbc.queryForString(connection, "select status from v$dblink_altilinker_status");
            assumeEnabled(
                    status != null && !status.isBlank(),
                    "databaseLink",
                    "AltiLinker status is not visible; start AltiLinker with ALTER DATABASE LINKER START"
            );
        } catch (IllegalStateException e) {
            Assumptions.abort("Feature databaseLink skipped: AltiLinker status view is not readable or AltiLinker is not running. " +
                    "Required setup: connect as SYSDBA and run ALTER DATABASE LINKER START. Cause: " + rootMessage(e));
        }
    }

    public static void assumeDirectoryFileIoAvailable(TestConfig config, JdbcHelper jdbc, Connection connection) {
        assumeEnabled(
                config.features().directoryFileIo(),
                "directoryFileIo",
                "features.directoryFileIo must be true; DB server OS user must be able to read/write " + config.paths().workRoot()
        );

        String directoryName = DbTestSupport.uniqueName("QA_DIR_PROBE");
        String procedureName = DbTestSupport.uniqueName("QA_DIR_PROBE_PROC");
        String fileName = procedureName.toLowerCase(Locale.ROOT) + ".txt";
        try {
            jdbc.executeUpdate(connection, "create directory " + directoryName + " as '" + config.paths().workRoot() + "'");
            jdbc.executeUpdate(connection,
                    "create or replace procedure " + procedureName + " as " +
                            "f FILE_TYPE; " +
                            "begin " +
                            "f := FOPEN('" + directoryName + "', '" + fileName + "', 'W'); " +
                            "PUT_LINE(f, 'ALTIBASE DIRECTORY PROBE'); " +
                            "FCLOSE(f); " +
                            "end;");
            callProcedure(connection, procedureName);
        } catch (RuntimeException | SQLException e) {
            Assumptions.abort("Feature directoryFileIo skipped: DB server process cannot write through DIRECTORY " +
                    "to " + config.paths().workRoot() + ". Required setup: create the path on the DB host and grant " +
                    "read/write permission to the OS user running Altibase. Cause: " + rootMessage(e));
        } finally {
            dropProcedureQuietly(jdbc, connection, procedureName);
            dropDirectoryQuietly(jdbc, connection, directoryName);
        }
    }

    public static void assumeStoredPackageAvailable(
            TestConfig config,
            JdbcHelper jdbc,
            Connection connection,
            String packageName,
            String probeSql
    ) {
        assumeEnabled(
                config.features().storedPackages(),
                "storedPackages",
                "features.storedPackages must be true; run the Altibase package scripts such as packages/catproc.sql as SYS"
        );

        try {
            jdbc.query(connection, probeSql);
        } catch (IllegalStateException e) {
            Assumptions.abort("Feature storedPackages skipped: package " + packageName +
                    " is not installed or not executable by the current user. Required setup: run Altibase system " +
                    "package scripts as SYS and verify " + packageName + " from iSQL. Cause: " + rootMessage(e));
        }
    }

    public static void assumeUtlTcpAvailable(TestConfig config, JdbcHelper jdbc, Connection connection) {
        assumeEnabled(
                config.features().utlTcp(),
                "utlTcp",
                "features.utlTcp must be true; a TCP endpoint must listen at " +
                        config.network().tcpHost() + ":" + config.network().tcpPort()
        );
        assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "UTL_TCP",
                "select count(*) from dual"
        );
    }

    public static void assumeUtlSmtpAvailable(TestConfig config, JdbcHelper jdbc, Connection connection) {
        assumeEnabled(
                config.features().utlSmtp(),
                "utlSmtp",
                "features.utlSmtp must be true; an SMTP endpoint must listen at " +
                        config.network().smtpHost() + ":" + config.network().smtpPort()
        );
        assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "UTL_SMTP",
                "select count(*) from dual"
        );
    }

    private static void assumeEnabled(boolean enabled, String featureName, String setup) {
        Assumptions.assumeTrue(enabled, "Feature " + featureName + " skipped: " + setup);
    }

    private static void callProcedure(Connection connection, String procedureName) throws SQLException {
        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "()}")) {
            callableStatement.execute();
        }
    }

    private static void dropProcedureQuietly(JdbcHelper jdbc, Connection connection, String procedureName) {
        try {
            jdbc.executeUpdate(connection, "drop procedure " + procedureName);
        } catch (Exception ignored) {
        }
    }

    private static void dropDirectoryQuietly(JdbcHelper jdbc, Connection connection, String directoryName) {
        try {
            jdbc.executeUpdate(connection, "drop directory " + directoryName);
        } catch (Exception ignored) {
        }
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage();
    }
}
