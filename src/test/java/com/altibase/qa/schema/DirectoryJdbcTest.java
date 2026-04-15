package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DirectoryJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_284_001 create a user and grant directory privileges")
    void tc284001CreateUserAndGrantDirectoryPrivileges() throws Exception {
        String userName = DbTestSupport.uniqueName("QA_DIR_USER");
        String directoryName = DbTestSupport.uniqueName("QA_DIR_PRIV");

        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);
        grantIfNeeded(userName, "create any directory");
        grantIfNeeded(userName, "drop any directory");

        withUserConnection(userName, conn -> {
            jdbc.executeUpdate(conn,
                    "create directory " + directoryName + " as '/tmp/altibase-qa-auto/" + directoryName.toLowerCase() + "'");
            assertThat(directoryExists(directoryName)).isTrue();
            jdbc.executeUpdate(conn, "drop directory " + directoryName);
        });

        assertThat(directoryExists(directoryName)).isFalse();
    }

    @Test
    @DisplayName("TC_285_001 create sample data first and then create a directory object")
    void tc285001CreateDirectoryAfterLoadingSampleData() throws Exception {
        String userName = DbTestSupport.uniqueName("QA_DIR_DATA_USER");
        String tableName = DbTestSupport.uniqueName("QA_DIR_DATA_TB");
        String directoryName = DbTestSupport.uniqueName("QA_DIR_DATA");

        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);
        grantIfNeeded(userName, "create table");
        grantIfNeeded(userName, "create any directory");

        withUserConnection(userName, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer, title varchar(40))");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1, 'ALTIBASE')");
            jdbc.executeUpdate(conn, "create directory " + directoryName + " as '/tmp/altibase-qa-auto'");

            assertThat(jdbc.queryForString(conn, "select title from " + tableName + " where id = 1")).isEqualTo("ALTIBASE");
        });

        assertThat(directoryExists(directoryName)).isTrue();
    }

    @Test
    @DisplayName("TC_286_001 create a stored procedure that writes a line through a directory object")
    void tc286001CreateWriteProcedureUsingDirectory() throws Exception {
        String userName = DbTestSupport.uniqueName("QA_DIR_PROC_USER");
        String directoryName = DbTestSupport.uniqueName("QA_DIR_WRITE");
        String procedureName = DbTestSupport.uniqueName("WRITE_T1");
        String fileName = procedureName.toLowerCase() + ".txt";

        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, userName + "." + procedureName));
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);
        grantIfNeeded(userName, "create procedure");
        grantIfNeeded(userName, "create any directory");

        withUserConnection(userName, conn -> {
            jdbc.executeUpdate(conn, "create directory " + directoryName + " as '/tmp/altibase-qa-auto'");
            jdbc.executeUpdate(conn,
                    "create or replace procedure " + procedureName + " as " +
                            "f FILE_TYPE; " +
                            "begin " +
                            "  f := FOPEN('" + directoryName + "', '" + fileName + "', 'W'); " +
                            "  PUT_LINE(f, 'ALTIBASE DIRECTORY WRITE'); " +
                            "  FCLOSE(f); " +
                            "end;");
            callProcedure(conn, procedureName);
        });

        assertThat(storedProgramExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_286_002 create a stored procedure that reads a line through a directory object")
    void tc286002CreateReadProcedureUsingDirectory() throws Exception {
        String userName = DbTestSupport.uniqueName("QA_DIR_READ_USER");
        String directoryName = DbTestSupport.uniqueName("QA_DIR_READ");
        String writeProcedure = DbTestSupport.uniqueName("WRITE_T1");
        String readProcedure = DbTestSupport.uniqueName("READ_T1");
        String fileName = readProcedure.toLowerCase() + ".txt";

        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, userName + "." + writeProcedure));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, userName + "." + readProcedure));
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);
        grantIfNeeded(userName, "create procedure");
        grantIfNeeded(userName, "create any directory");

        withUserConnection(userName, conn -> {
            jdbc.executeUpdate(conn, "create directory " + directoryName + " as '/tmp/altibase-qa-auto'");
            jdbc.executeUpdate(conn,
                    "create or replace procedure " + writeProcedure + " as " +
                            "f FILE_TYPE; " +
                            "begin " +
                            "  f := FOPEN('" + directoryName + "', '" + fileName + "', 'W'); " +
                            "  PUT_LINE(f, 'ALTIBASE DIRECTORY READ'); " +
                            "  FCLOSE(f); " +
                            "end;");
            jdbc.executeUpdate(conn,
                    "create or replace procedure " + readProcedure + " as " +
                            "f FILE_TYPE; " +
                            "line varchar(200); " +
                            "begin " +
                            "  f := FOPEN('" + directoryName + "', '" + fileName + "', 'R'); " +
                            "  GET_LINE(f, line); " +
                            "  PRINT(line); " +
                            "  FCLOSE(f); " +
                            "end;");

            callProcedure(conn, writeProcedure);
            callProcedure(conn, readProcedure);
        });

        assertThat(storedProgramExists(writeProcedure)).isTrue();
        assertThat(storedProgramExists(readProcedure)).isTrue();
    }

    @Test
    @DisplayName("TC_281_001 create a directory object")
    void tc281001CreateDirectory() {
        String directoryName = DbTestSupport.uniqueName("QA_DIR");
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create directory " + directoryName + " as '/tmp/altibase-qa-auto/" + directoryName.toLowerCase() + "'");

        assertThat(directoryExists(directoryName)).isTrue();
    }

    @Test
    @DisplayName("TC_282_001 create or replace directory changes the mapped path")
    void tc282001CreateOrReplaceDirectory() {
        String directoryName = DbTestSupport.uniqueName("QA_DIR");
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create directory " + directoryName + " as '/tmp/altibase-qa-auto/a'");
        jdbc.executeUpdate(connection, "create or replace directory " + directoryName + " as '/tmp/altibase-qa-auto/b'");

        assertThat(jdbc.queryForString(connection,
                "select directory_path from system_.sys_directories_ where directory_name = '" + directoryName + "'"))
                .isEqualTo("/tmp/altibase-qa-auto/b");
    }

    @Test
    @DisplayName("TC_283_001 drop directory removes the directory object")
    void tc283001DropDirectory() {
        String directoryName = DbTestSupport.uniqueName("QA_DIR");

        jdbc.executeUpdate(connection, "create directory " + directoryName + " as '/tmp/altibase-qa-auto/" + directoryName.toLowerCase() + "'");
        jdbc.executeUpdate(connection, "drop directory " + directoryName);

        assertThat(directoryExists(directoryName)).isFalse();
    }

    @Test
    @DisplayName("Additional negative case: duplicate directory names fail without REPLACE")
    void duplicateDirectoryFailsWithoutReplace() {
        String directoryName = DbTestSupport.uniqueName("QA_DIR");
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create directory " + directoryName + " as '/tmp/altibase-qa-auto/a'");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "create directory " + directoryName + " as '/tmp/altibase-qa-auto/b'"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: directory creation without privilege is rejected")
    void createDirectoryWithoutPrivilegeFails() throws Exception {
        String userName = DbTestSupport.uniqueName("QA_DIR_NOPRIV_USER");
        String directoryName = DbTestSupport.uniqueName("QA_DIR_NOPRIV");

        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);

        withUserConnection(userName, conn ->
                assertThatThrownBy(() ->
                        jdbc.executeUpdate(conn, "create directory " + directoryName + " as '/tmp/altibase-qa-auto'"))
                        .isInstanceOf(IllegalStateException.class));
    }

    @Test
    @DisplayName("Additional negative case: a directory-backed procedure fails after the directory is dropped")
    void directoryBackedProcedureFailsAfterDirectoryDrop() throws Exception {
        String userName = DbTestSupport.uniqueName("QA_DIR_DROP_USER");
        String directoryName = DbTestSupport.uniqueName("QA_DIR_DROP");
        String procedureName = DbTestSupport.uniqueName("WRITE_DROP_T1");
        String fileName = procedureName.toLowerCase() + ".txt";

        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, userName + "." + procedureName));
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);
        grantIfNeeded(userName, "create procedure");
        grantIfNeeded(userName, "create any directory");

        withUserConnection(userName, conn -> {
            jdbc.executeUpdate(conn, "create directory " + directoryName + " as '/tmp/altibase-qa-auto'");
            jdbc.executeUpdate(conn,
                    "create or replace procedure " + procedureName + " as " +
                            "f FILE_TYPE; " +
                            "begin " +
                            "  f := FOPEN('" + directoryName + "', '" + fileName + "', 'W'); " +
                            "  PUT_LINE(f, 'ALTIBASE DIRECTORY DROP'); " +
                            "  FCLOSE(f); " +
                            "end;");
            jdbc.executeUpdate(connection, "drop directory " + directoryName);

            assertThatThrownBy(() -> callProcedure(conn, procedureName))
                    .isInstanceOf(IllegalStateException.class);
        });
    }

    private boolean directoryExists(String directoryName) {
        return jdbc.exists(connection,
                "select directory_name from system_.sys_directories_ where directory_name = '" + directoryName + "'");
    }

    private boolean storedProgramExists(String procedureName) {
        return jdbc.exists(connection,
                "select proc_name from system_.sys_procedures_ where proc_name = '" + procedureName + "'");
    }

    private void dropDirectoryQuietly(String directoryName) {
        try {
            jdbc.executeUpdate(connection, "drop directory " + directoryName);
        } catch (Exception ignored) {
        }
    }

    private void callProcedure(Connection connection, String procedureName) {
        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "()}")) {
            callableStatement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to call procedure: " + procedureName, e);
        }
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

    private void withUserConnection(String userName, SqlConnectionConsumer consumer) throws Exception {
        Connection userConnection = jdbc.open(userName, userName);
        try {
            consumer.accept(userConnection);
        } finally {
            jdbc.closeQuietly(userConnection);
        }
    }

    @FunctionalInterface
    private interface SqlConnectionConsumer {
        void accept(Connection connection) throws Exception;
    }
}
