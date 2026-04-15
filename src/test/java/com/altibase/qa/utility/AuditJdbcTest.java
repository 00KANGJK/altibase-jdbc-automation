package com.altibase.qa.utility;

import com.altibase.qa.base.BaseDbTest;
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

    private String auditQuery(String columnName, String userName) {
        return "select " + columnName + " from system_.sys_audit_opts_ " +
                "where user_name = '" + userName + "' and object_name = 'ALL'";
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
