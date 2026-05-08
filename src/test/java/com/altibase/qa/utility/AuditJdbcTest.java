package com.altibase.qa.utility;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AuditJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_361_001 NOAUDIT removes statement audit conditions")
    void tc361001NoauditRemovesStatementAudit() {
        registerCleanup(this::clearGlobalStatementAudit);

        clearGlobalStatementAudit();
        jdbc.executeUpdate(connection, "audit insert, update, delete");

        assertThat(jdbc.queryForString(connection, auditQuery("INSERT_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("UPDATE_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("DELETE_OP", "ALL"))).isEqualTo("S/S");

        jdbc.executeUpdate(connection, "noaudit insert");

        assertThat(jdbc.queryForString(connection, auditQuery("INSERT_OP", "ALL"))).isEqualTo("-/-");
        assertThat(jdbc.queryForString(connection, auditQuery("UPDATE_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("DELETE_OP", "ALL"))).isEqualTo("S/S");
    }

    @Test
    @DisplayName("TC_362_001 DELAUDIT removes user-specific audit conditions")
    void tc362001DelauditRemovesUserSpecificConditions() {
        registerCleanup(() -> clearUserAudit("SYS"));

        clearUserAudit("SYS");
        jdbc.executeUpdate(connection, "audit insert, update, delete by sys");

        assertThat(jdbc.query(connection,
                "select user_name, insert_op, update_op, delete_op from system_.sys_audit_opts_ where user_name = 'SYS'").rows())
                .hasSize(1);

        jdbc.executeUpdate(connection, "delaudit by sys");

        assertThat(jdbc.queryForString(connection,
                "select count(*) from system_.sys_audit_opts_ where user_name = 'SYS'")).isEqualTo("0");
    }

    @Test
    @DisplayName("TC_359_001 AUDIT enables statement audit for specific SQL operations")
    void tc359001AuditEnablesStatementAuditForSpecificOps() {
        registerCleanup(this::clearGlobalStatementAudit);

        clearGlobalStatementAudit();
        jdbc.executeUpdate(connection, "audit select, insert, update, delete, execute");

        assertThat(jdbc.queryForString(connection, auditQuery("SELECT_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("INSERT_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("UPDATE_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("DELETE_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("EXECUTE_OP", "ALL"))).isEqualTo("S/S");
    }

    @Test
    @DisplayName("TC_359_002 AUDIT enables DELETE, MOVE, and MERGE statement audit")
    void tc359002AuditDeleteMoveMergeStatements() {
        String operations = "delete, move, merge";
        registerCleanup(() -> clearAuditOperations(operations));

        clearAuditOperations(operations);
        jdbc.executeUpdate(connection, "audit " + operations);

        assertAuditColumnsEnabled("DELETE_OP", "MOVE_OP", "MERGE_OP");
    }

    @Test
    @DisplayName("TC_359_003 AUDIT enables queue, lock, and execute statement audit")
    void tc359003AuditQueueLockExecuteStatements() {
        String operations = "enqueue, dequeue, lock, execute";
        registerCleanup(() -> clearAuditOperations(operations));

        clearAuditOperations(operations);
        jdbc.executeUpdate(connection, "audit " + operations);

        assertAuditColumnsEnabled("ENQUEUE_OP", "DEQUEUE_OP", "LOCK_TABLE_OP", "EXECUTE_OP");
    }

    @Test
    @DisplayName("TC_359_004 AUDIT enables transaction and connect statement audit")
    void tc359004AuditTransactionAndConnectStatements() {
        String operations = "commit, rollback, savepoint, connect";
        registerCleanup(() -> clearAuditOperations(operations));

        clearAuditOperations(operations);
        jdbc.executeUpdate(connection, "audit " + operations);

        assertAuditColumnsEnabled("COMMIT_OP", "ROLLBACK_OP", "SAVEPOINT_OP", "CONNECT_OP");
    }

    @Test
    @DisplayName("TC_359_005 AUDIT enables disconnect and ALTER statement audit")
    void tc359005AuditDisconnectAndAlterStatements() {
        String operations = "disconnect, alter session, alter system";
        registerCleanup(() -> clearAuditOperations(operations));

        clearAuditOperations(operations);
        jdbc.executeUpdate(connection, "audit " + operations);

        assertAuditColumnsEnabled("DISCONNECT_OP", "ALTER_SESSION_OP", "ALTER_SYSTEM_OP");
    }

    @Test
    @DisplayName("TC_359_006 AUDIT DDL enables DDL statement audit")
    void tc359006AuditDdlStatements() {
        registerCleanup(() -> {
            try { jdbc.executeUpdate(connection, "noaudit connect, disconnect"); } catch (Exception ignored) {}
        });

        try { jdbc.executeUpdate(connection, "noaudit connect, disconnect"); } catch (Exception ignored) {}
        jdbc.executeUpdate(connection, "audit connect, disconnect");

        assertThat(jdbc.queryForString(connection, auditQuery("CONNECT_OP", "ALL"))).isEqualTo("T/T");
        assertThat(jdbc.queryForString(connection, auditQuery("DISCONNECT_OP", "ALL"))).isEqualTo("T/T");
    }

    @Test
    @DisplayName("TC_360_001 AUDIT ALL enables all audit targets and records all operations")
    void tc360001AuditAllEnablesAllTargets() {
        registerCleanup(this::clearGlobalStatementAudit);

        clearGlobalStatementAudit();
        jdbc.executeUpdate(connection, "audit all");

        assertThat(jdbc.queryForString(connection, auditQuery("SELECT_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("INSERT_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("UPDATE_OP", "ALL"))).isEqualTo("S/S");
        assertThat(jdbc.queryForString(connection, auditQuery("DELETE_OP", "ALL"))).isEqualTo("S/S");
    }

    private String auditQuery(String columnName, String userName) {
        return "select " + columnName + " from system_.sys_audit_opts_ " +
                "where user_name = '" + userName + "' and object_name = 'ALL'";
    }

    private void assertAuditColumnsEnabled(String... columnNames) {
        QueryResult result = jdbc.query(
                connection,
                "select " + String.join(", ", columnNames) +
                        " from system_.sys_audit_opts_ where user_name = 'ALL' and object_name = 'ALL'"
        );

        assertThat(result.size()).isEqualTo(1);
        for (String columnName : columnNames) {
            assertThat(String.valueOf(result.value(0, columnName))).isNotEqualTo("-/-");
        }
    }

    private void clearAuditOperations(String operations) {
        try {
            jdbc.executeUpdate(connection, "noaudit " + operations);
        } catch (Exception ignored) {
        }
    }

    private void clearGlobalStatementAudit() {
        try {
            jdbc.executeUpdate(connection, "noaudit insert, update, delete");
        } catch (Exception ignored) {
        }
    }

    private void clearUserAudit(String userName) {
        try {
            jdbc.executeUpdate(connection, "delaudit by " + userName);
        } catch (Exception ignored) {
        }
    }
}
