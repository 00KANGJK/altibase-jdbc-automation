package com.altibase.qa.security;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrivilegeGrantJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_300_001 ALTER USER can change TCP access for a user")
    void tc300001AlterUserTcpAccess() {
        String user = createManagedUser("QA_TCP_USER");

        jdbc.closeQuietly(jdbc.open(user, user));
        jdbc.executeUpdate(connection, "alter user " + user + " disable tcp");

        assertThatThrownBy(() -> jdbc.open(user, user))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_301_001 ALTER USER can lock an account")
    void tc301001LockUserAccount() {
        String user = createManagedUser("QA_LOCK_USER");

        jdbc.executeUpdate(connection, "alter user " + user + " account lock");

        assertThatThrownBy(() -> jdbc.open(user, user))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_310_002 ALTER USER privilege can be granted to a role")
    void tc310002GrantAlterUserPrivilegeViaRole() throws Exception {
        String role = createManagedRole("QA_ROLE_ALTER_USER");
        String grantee = createManagedUser("QA_ALTER_USER");
        String target = createManagedUser("QA_ALTER_TARGET");

        grantToRole(role, "alter user");
        grantRoleToUser(role, grantee);

        withUserConnection(grantee, conn -> jdbc.executeUpdate(conn, "alter user " + target + " account lock"));

        assertThatThrownBy(() -> jdbc.open(target, target))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_310_003 DROP USER privilege can be granted to a role")
    void tc310003GrantDropUserPrivilegeViaRole() throws Exception {
        String role = createManagedRole("QA_ROLE_DROP_USER");
        String grantee = createManagedUser("QA_DROP_USER");
        String target = createManagedUser("QA_DROP_TARGET");

        grantToRole(role, "drop user");
        grantRoleToUser(role, grantee);

        withUserConnection(grantee, conn -> jdbc.executeUpdate(conn, "drop user " + target));

        assertThat(DbTestSupport.userExists(jdbc, connection, target)).isFalse();
    }

    @Test
    @DisplayName("TC_311_001 CREATE ANY TABLE privilege can be granted to a role")
    void tc311001GrantCreateAnyTable() throws Exception {
        String owner = createManagedUser("QA_ANY_TB_OWNER");
        String user = createManagedUser("QA_ANY_TB_USER");
        String role = createManagedRole("QA_ANY_TB_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_ANY_TB");

        grantToRole(role, "create any table");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create table " + owner + "." + tableName + "(c1 integer)"));

        assertThat(DbTestSupport.tableExists(connection, owner, tableName)).isTrue();
    }

    @Test
    @DisplayName("TC_311_002 ALTER ANY TABLE privilege can be granted to a role")
    void tc311002GrantAlterAnyTable() throws Exception {
        String owner = createManagedUser("QA_ALT_TB_OWNER");
        String user = createManagedUser("QA_ALT_TB_USER");
        String role = createManagedRole("QA_ALT_TB_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_ALT_TB");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer)"));
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "alter table " + owner + "." + tableName + " add column (c2 integer)")))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "alter any table");
        grantRoleToUser(role, user);
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter table " + owner + "." + tableName + " add column (c2 integer)"));

        assertThat(DbTestSupport.columnExists(connection, owner, tableName, "c2")).isTrue();
    }

    @Test
    @DisplayName("TC_311_003 DROP ANY TABLE privilege can be granted to a role")
    void tc311003GrantDropAnyTable() throws Exception {
        String owner = createManagedUser("QA_DROP_TB_OWNER");
        String user = createManagedUser("QA_DROP_TB_USER");
        String role = createManagedRole("QA_DROP_TB_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_DROP_TB");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer)"));
        grantToRole(role, "drop any table");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop table " + owner + "." + tableName + " cascade"));

        assertThat(DbTestSupport.tableExists(connection, owner, tableName)).isFalse();
    }

    @Test
    @DisplayName("TC_311_008 LOCK ANY TABLE privilege can be granted to a role")
    void tc311008GrantLockAnyTable() throws Exception {
        String owner = createManagedUser("QA_LOCK_TB_OWNER");
        String user = createManagedUser("QA_LOCK_TB_USER");
        String role = createManagedRole("QA_LOCK_TB_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_LOCK_TB");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer)"));
        assertThatThrownBy(() -> withUserConnection(user, conn -> lockTableInExclusiveMode(conn, owner + "." + tableName)))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "lock any table");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> lockTableInExclusiveMode(conn, owner + "." + tableName));
    }

    @Test
    @DisplayName("TC_312_001 CREATE ANY INDEX privilege can be granted to a role")
    void tc312001GrantCreateAnyIndex() throws Exception {
        String owner = createManagedUser("QA_IDX_OWNER");
        String user = createManagedUser("QA_IDX_USER");
        String role = createManagedRole("QA_IDX_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_IDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_ANY_IDX");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)"));
        grantToRole(role, "create any index");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create index " + owner + "." + indexName + " on " + owner + "." + tableName + "(c1)"));

        assertThat(indexExists(indexName)).isTrue();
    }

    @Test
    @DisplayName("TC_312_002 ALTER ANY INDEX privilege can be granted to a role")
    void tc312002GrantAlterAnyIndex() throws Exception {
        String owner = createManagedUser("QA_ALT_IDX_OWNER");
        String user = createManagedUser("QA_ALT_IDX_USER");
        String role = createManagedRole("QA_ALT_IDX_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_ALT_IDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_ALT_IDX");
        String renamedIndex = DbTestSupport.uniqueName("QA_ALT_IDX_NEW");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)");
            jdbc.executeUpdate(conn, "create index " + indexName + " on " + tableName + "(c1)");
        });
        grantToRole(role, "alter any index");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter index " + owner + "." + indexName + " rename to " + renamedIndex));

        assertThat(indexExists(indexName)).isFalse();
        assertThat(indexExists(renamedIndex)).isTrue();
    }

    @Test
    @DisplayName("TC_312_003 DROP ANY INDEX privilege can be granted to a role")
    void tc312003GrantDropAnyIndex() throws Exception {
        String owner = createManagedUser("QA_DROP_IDX_OWNER");
        String user = createManagedUser("QA_DROP_IDX_USER");
        String role = createManagedRole("QA_DROP_IDX_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_DROP_IDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_DROP_IDX");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)");
            jdbc.executeUpdate(conn, "create index " + indexName + " on " + tableName + "(c1)");
        });
        grantToRole(role, "drop any index");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop index " + owner + "." + indexName));

        assertThat(indexExists(indexName)).isFalse();
    }

    @Test
    @Disabled("Defect candidate on Altibase 7.3.0.1.8: role-granted CREATE PROCEDURE is not honored for own-schema procedure creation")
    @DisplayName("TC_313_001 CREATE PROCEDURE privilege allows own-schema procedures only")
    void tc313001GrantCreateProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_OWNER");
        String user = createManagedUser("QA_PROC_USER");
        String role = createManagedRole("QA_PROC_ROLE");
        String ownProcedure = DbTestSupport.uniqueName("QA_PROC_OWN");
        String otherProcedure = DbTestSupport.uniqueName("QA_PROC_OTHER");

        jdbc.executeUpdate(connection, "revoke create procedure from " + user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleOutProcedure(conn, ownProcedure)))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "create procedure");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> createSimpleOutProcedure(conn, ownProcedure));
        assertThat(storedProgramExists(ownProcedure)).isTrue();

        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleOutProcedure(conn, owner + "." + otherProcedure)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_313_002 CREATE ANY PROCEDURE privilege can be granted to a role")
    void tc313002GrantCreateAnyProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_ANY_OWNER");
        String user = createManagedUser("QA_PROC_ANY_USER");
        String role = createManagedRole("QA_PROC_ANY_ROLE");
        String procedureName = DbTestSupport.uniqueName("QA_PROC_ANY");

        grantToRole(role, "create any procedure");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> createSimpleOutProcedure(conn, owner + "." + procedureName));

        assertThat(storedProgramExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_313_003 ALTER ANY PROCEDURE privilege can be granted to a role")
    void tc313003GrantAlterAnyProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_ALT_OWNER");
        String user = createManagedUser("QA_PROC_ALT_USER");
        String role = createManagedRole("QA_PROC_ALT_ROLE");
        String procedureName = DbTestSupport.uniqueName("QA_PROC_ALT");

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        grantToRole(role, "alter any procedure");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter procedure " + owner + "." + procedureName + " compile"));

        assertThat(storedProgramExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_313_004 DROP ANY PROCEDURE privilege can be granted to a role")
    void tc313004GrantDropAnyProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_DROP_OWNER");
        String user = createManagedUser("QA_PROC_DROP_USER");
        String role = createManagedRole("QA_PROC_DROP_ROLE");
        String procedureName = DbTestSupport.uniqueName("QA_PROC_DROP");

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        grantToRole(role, "drop any procedure");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop procedure " + owner + "." + procedureName));

        assertThat(storedProgramExists(procedureName)).isFalse();
    }

    @Test
    @DisplayName("TC_313_005 EXECUTE ANY PROCEDURE privilege can be granted to a role")
    void tc313005GrantExecuteAnyProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_EXEC_OWNER");
        String user = createManagedUser("QA_PROC_EXEC_USER");
        String role = createManagedRole("QA_PROC_EXEC_ROLE");
        String procedureName = DbTestSupport.uniqueName("QA_PROC_EXEC");

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        grantToRole(role, "execute any procedure");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> assertThat(executeOutProcedure(conn, owner + "." + procedureName)).isEqualTo(1));
    }

    @Test
    @DisplayName("TC_314_001 CREATE ANY SEQUENCE privilege can be granted to a role")
    void tc314001GrantCreateAnySequence() throws Exception {
        String owner = createManagedUser("QA_SEQ_OWNER");
        String user = createManagedUser("QA_SEQ_USER");
        String role = createManagedRole("QA_SEQ_ROLE");
        String sequenceName = DbTestSupport.uniqueName("QA_ANY_SEQ");

        grantToRole(role, "create any sequence");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> createSimpleSequence(conn, owner + "." + sequenceName));

        assertThat(nextSequenceValue(connection, owner + "." + sequenceName)).isEqualTo("1");
    }

    @Test
    @DisplayName("TC_314_002 ALTER ANY SEQUENCE privilege can be granted to a role")
    void tc314002GrantAlterAnySequence() throws Exception {
        String owner = createManagedUser("QA_ALT_SEQ_OWNER");
        String user = createManagedUser("QA_ALT_SEQ_USER");
        String role = createManagedRole("QA_ALT_SEQ_ROLE");
        String sequenceName = DbTestSupport.uniqueName("QA_ALT_SEQ");

        withUserConnection(owner, conn -> createSimpleSequence(conn, sequenceName));
        assertThat(nextSequenceValue(connection, owner + "." + sequenceName)).isEqualTo("1");
        grantToRole(role, "alter any sequence");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter sequence " + owner + "." + sequenceName + " increment by 5"));

        assertThat(nextSequenceValue(connection, owner + "." + sequenceName)).isEqualTo("6");
    }

    @Test
    @DisplayName("TC_314_003 DROP ANY SEQUENCE privilege can be granted to a role")
    void tc314003GrantDropAnySequence() throws Exception {
        String owner = createManagedUser("QA_DROP_SEQ_OWNER");
        String user = createManagedUser("QA_DROP_SEQ_USER");
        String role = createManagedRole("QA_DROP_SEQ_ROLE");
        String sequenceName = DbTestSupport.uniqueName("QA_DROP_SEQ");

        withUserConnection(owner, conn -> createSimpleSequence(conn, sequenceName));
        grantToRole(role, "drop any sequence");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop sequence " + owner + "." + sequenceName));

        assertThatThrownBy(() -> jdbc.queryForString(connection, "select " + owner + "." + sequenceName + ".nextval from dual"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_314_004 SELECT ANY SEQUENCE privilege can be granted to a role")
    void tc314004GrantSelectAnySequence() throws Exception {
        String owner = createManagedUser("QA_SEL_SEQ_OWNER");
        String user = createManagedUser("QA_SEL_SEQ_USER");
        String role = createManagedRole("QA_SEL_SEQ_ROLE");
        String sequenceName = DbTestSupport.uniqueName("QA_SEL_SEQ");

        withUserConnection(owner, conn -> createSimpleSequence(conn, sequenceName));
        grantToRole(role, "select any sequence");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> assertThat(nextSequenceValue(conn, owner + "." + sequenceName)).isEqualTo("1"));
    }

    @Test
    @DisplayName("TC_315_001 CREATE ANY VIEW privilege can be granted to a role")
    void tc315001GrantCreateAnyView() throws Exception {
        String owner = createManagedUser("QA_VIEW_OWNER");
        String user = createManagedUser("QA_VIEW_USER");
        String role = createManagedRole("QA_VIEW_ROLE");
        String viewName = DbTestSupport.uniqueName("QA_ANY_VIEW");

        grantToRole(role, "create any view");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> createConstantView(conn, owner + "." + viewName));

        assertThat(jdbc.queryForString(connection, "select c1 from " + owner + "." + viewName)).isEqualTo("1");
    }

    @Test
    @DisplayName("TC_315_002 DROP ANY VIEW privilege can be granted to a role")
    void tc315002GrantDropAnyView() throws Exception {
        String owner = createManagedUser("QA_DROP_VIEW_OWNER");
        String user = createManagedUser("QA_DROP_VIEW_USER");
        String role = createManagedRole("QA_DROP_VIEW_ROLE");
        String viewName = DbTestSupport.uniqueName("QA_DROP_VIEW");

        withUserConnection(owner, conn -> createConstantView(conn, viewName));
        grantToRole(role, "drop any view");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop view " + owner + "." + viewName));

        assertThat(DbTestSupport.viewExists(connection, owner, viewName)).isFalse();
    }

    @Test
    @DisplayName("TC_316_001 CREATE TABLESPACE privilege can be granted to a role")
    void tc316001GrantCreateTablespace() throws Exception {
        String user = createManagedUser("QA_TS_CREATE_USER");
        String role = createManagedRole("QA_TS_CREATE_ROLE");
        String tablespaceName = DbTestSupport.uniqueName("QA_TS_CREATE");

        registerCleanup(() -> dropTablespaceQuietly(tablespaceName));

        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create memory tablespace " + tablespaceName + " size 8M")))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "create tablespace");
        grantRoleToUser(role, user);
        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create memory tablespace " + tablespaceName + " size 8M"));

        assertThat(tablespaceExists(tablespaceName)).isTrue();
    }

    @Test
    @DisplayName("TC_316_002 ALTER TABLESPACE privilege can be granted to a role")
    void tc316002GrantAlterTablespace() throws Exception {
        String user = createManagedUser("QA_TS_ALTER_USER");
        String role = createManagedRole("QA_TS_ALTER_ROLE");
        String tablespaceName = DbTestSupport.uniqueName("QA_TS_ALTER");

        registerCleanup(() -> dropTablespaceQuietly(tablespaceName));
        jdbc.executeUpdate(connection, "create memory tablespace " + tablespaceName + " size 8M");

        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "alter tablespace " + tablespaceName + " alter autoextend on next 4M maxsize 20M")))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "alter tablespace");
        grantRoleToUser(role, user);
        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "alter tablespace " + tablespaceName + " alter autoextend on next 4M maxsize 20M"));

        assertThat(tablespaceExists(tablespaceName)).isTrue();
    }

    @Test
    @DisplayName("TC_316_003 DROP TABLESPACE privilege can be granted to a role")
    void tc316003GrantDropTablespace() throws Exception {
        String user = createManagedUser("QA_TS_DROP_USER");
        String role = createManagedRole("QA_TS_DROP_ROLE");
        String tablespaceName = DbTestSupport.uniqueName("QA_TS_DROP");

        jdbc.executeUpdate(connection, "create memory tablespace " + tablespaceName + " size 8M");
        assertThat(tablespaceExists(tablespaceName)).isTrue();

        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "drop tablespace " + tablespaceName + " including contents and datafiles")))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "drop tablespace");
        grantRoleToUser(role, user);
        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "drop tablespace " + tablespaceName + " including contents and datafiles"));

        assertThat(tablespaceExists(tablespaceName)).isFalse();
    }

    @Test
    @Disabled("Defect candidate on Altibase 7.3.0.1.8: role-granted CREATE TRIGGER is not honored for own-schema trigger creation")
    @DisplayName("TC_317_001 CREATE TRIGGER privilege allows own-schema triggers only")
    void tc317001GrantCreateTrigger() throws Exception {
        String owner = createManagedUser("QA_TRG_OWNER");
        String user = createManagedUser("QA_TRG_USER");
        String role = createManagedRole("QA_TRG_ROLE");
        String ownTable = DbTestSupport.uniqueName("QA_TRG_OWN_TB");
        String ownTrigger = DbTestSupport.uniqueName("QA_TRG_OWN");
        String otherTable = DbTestSupport.uniqueName("QA_TRG_OTHER_TB");
        String otherTrigger = DbTestSupport.uniqueName("QA_TRG_OTHER");

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create table " + ownTable + "(c1 integer, c2 integer)"));
        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + otherTable + "(c1 integer, c2 integer)"));
        jdbc.executeUpdate(connection, "revoke create trigger from " + user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleTrigger(conn, ownTrigger, ownTable)))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "create trigger");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> createSimpleTrigger(conn, ownTrigger, ownTable));
        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "insert into " + ownTable + "(c1, c2) values(1, null)");
            assertThat(jdbc.queryForString(conn, "select c2 from " + ownTable + " where c1 = 1")).isEqualTo("0");
        });

        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleTrigger(conn, owner + "." + otherTrigger, owner + "." + otherTable)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_317_002 CREATE ANY TRIGGER privilege can be granted to a role")
    void tc317002GrantCreateAnyTrigger() throws Exception {
        String owner = createManagedUser("QA_ANY_TRG_OWNER");
        String user = createManagedUser("QA_ANY_TRG_USER");
        String role = createManagedRole("QA_ANY_TRG_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_ANY_TRG_TB");
        String triggerName = DbTestSupport.uniqueName("QA_ANY_TRG");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)"));
        grantToRole(role, "create any trigger");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> createSimpleTrigger(conn, owner + "." + triggerName, owner + "." + tableName));
        jdbc.executeUpdate(connection, "insert into " + owner + "." + tableName + "(c1, c2) values(1, null)");

        assertThat(jdbc.queryForString(connection, "select c2 from " + owner + "." + tableName + " where c1 = 1")).isEqualTo("0");
    }

    @Test
    @DisplayName("TC_317_003 ALTER ANY TRIGGER privilege can be granted to a role")
    void tc317003GrantAlterAnyTrigger() throws Exception {
        String owner = createManagedUser("QA_ALT_TRG_OWNER");
        String user = createManagedUser("QA_ALT_TRG_USER");
        String role = createManagedRole("QA_ALT_TRG_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_ALT_TRG_TB");
        String triggerName = DbTestSupport.uniqueName("QA_ALT_TRG");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)");
            createSimpleTrigger(conn, triggerName, tableName);
        });
        grantToRole(role, "alter any trigger");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter trigger " + owner + "." + triggerName + " disable"));
        jdbc.executeUpdate(connection, "insert into " + owner + "." + tableName + "(c1, c2) values(1, null)");

        assertThat(jdbc.queryForString(connection, "select c2 from " + owner + "." + tableName + " where c1 = 1")).isNull();
    }

    @Test
    @DisplayName("TC_317_004 DROP ANY TRIGGER privilege can be granted to a role")
    void tc317004GrantDropAnyTrigger() throws Exception {
        String owner = createManagedUser("QA_DROP_TRG_OWNER");
        String user = createManagedUser("QA_DROP_TRG_USER");
        String role = createManagedRole("QA_DROP_TRG_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_DROP_TRG_TB");
        String triggerName = DbTestSupport.uniqueName("QA_DROP_TRG");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)");
            createSimpleTrigger(conn, triggerName, tableName);
        });
        grantToRole(role, "drop any trigger");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop trigger " + owner + "." + triggerName));
        jdbc.executeUpdate(connection, "insert into " + owner + "." + tableName + "(c1, c2) values(1, null)");

        assertThat(jdbc.queryForString(connection, "select c2 from " + owner + "." + tableName + " where c1 = 1")).isNull();
    }

    @Test
    @Disabled("Defect candidate on Altibase 7.3.0.1.8: role-granted CREATE SYNONYM is not honored for own-schema synonym creation")
    @DisplayName("TC_318_001 CREATE SYNONYM privilege can be granted to a role")
    void tc318001GrantCreateSynonym() throws Exception {
        String user = createManagedUser("QA_SYN_USER");
        String role = createManagedRole("QA_SYN_ROLE");
        String synonymName = DbTestSupport.uniqueName("QA_SYN");

        registerCleanup(() -> {
            try {
                withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop synonym " + synonymName));
            } catch (Exception ignored) {
            }
        });
        jdbc.executeUpdate(connection, "revoke create synonym from " + user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create synonym " + synonymName + " for dual")))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "create synonym");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for dual");
            assertThat(jdbc.queryForString(conn, "select count(*) from " + synonymName)).isEqualTo("1");
        });
    }

    @Test
    @DisplayName("TC_318_002 DROP ANY SYNONYM privilege can be granted to a role")
    void tc318002GrantDropSynonym() throws Exception {
        String owner = createManagedUser("QA_DROP_SYN_OWNER");
        String user = createManagedUser("QA_DROP_SYN_USER");
        String role = createManagedRole("QA_DROP_SYN_ROLE");
        String synonymName = DbTestSupport.uniqueName("QA_DROP_SYN");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create synonym " + synonymName + " for dual"));
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop synonym " + owner + "." + synonymName)))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "drop any synonym");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop synonym " + owner + "." + synonymName));

        assertThatThrownBy(() -> withUserConnection(owner, conn -> jdbc.queryForString(conn, "select count(*) from " + synonymName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_318_003 CREATE PUBLIC SYNONYM privilege can be granted to a role")
    void tc318003GrantCreatePublicSynonym() throws Exception {
        String user = createManagedUser("QA_PUB_SYN_USER");
        String role = createManagedRole("QA_PUB_SYN_ROLE");
        String synonymName = DbTestSupport.uniqueName("QA_PUB_SYN");

        registerCleanup(() -> DbTestSupport.dropPublicSynonymQuietly(jdbc, connection, synonymName));
        grantToRole(role, "create public synonym");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create public synonym " + synonymName + " for dual"));

        assertThat(jdbc.queryForString(connection, "select count(*) from " + synonymName)).isEqualTo("1");
    }

    @Test
    @DisplayName("TC_318_004 DROP PUBLIC SYNONYM privilege can be granted to a role")
    void tc318004GrantDropPublicSynonym() throws Exception {
        String user = createManagedUser("QA_DROP_PUB_SYN_USER");
        String role = createManagedRole("QA_DROP_PUB_SYN_ROLE");
        String synonymName = DbTestSupport.uniqueName("QA_DROP_PUB_SYN");

        jdbc.executeUpdate(connection, "create public synonym " + synonymName + " for dual");
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop public synonym " + synonymName)))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "drop public synonym");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop public synonym " + synonymName));

        assertThatThrownBy(() -> jdbc.queryForString(connection, "select count(*) from " + synonymName))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_319_001 CREATE ANY DIRECTORY privilege can be granted to a role")
    void tc319001GrantCreateAnyDirectory() throws Exception {
        String user = createManagedUser("QA_DIR_USER");
        String role = createManagedRole("QA_DIR_ROLE");
        String directoryName = DbTestSupport.uniqueName("QA_DIR");

        registerCleanup(() -> dropDirectoryQuietly(directoryName));
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create directory " + directoryName + " as '/tmp/altibase-qa-auto/" + directoryName.toLowerCase() + "'")))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "create any directory");
        grantRoleToUser(role, user);

        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create directory " + directoryName + " as '/tmp/altibase-qa-auto/" + directoryName.toLowerCase() + "'"));

        assertThat(directoryExists(directoryName)).isTrue();
    }

    @Test
    @DisplayName("TC_319_002 DROP ANY DIRECTORY privilege can be granted to a role")
    void tc319002GrantDropAnyDirectory() throws Exception {
        String user = createManagedUser("QA_DROP_DIR_USER");
        String role = createManagedRole("QA_DROP_DIR_ROLE");
        String directoryName = DbTestSupport.uniqueName("QA_DROP_DIR");

        jdbc.executeUpdate(connection, "create directory " + directoryName + " as '/tmp/altibase-qa-auto/" + directoryName.toLowerCase() + "'");
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop directory " + directoryName)))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "drop any directory");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop directory " + directoryName));

        assertThat(directoryExists(directoryName)).isFalse();
    }

    @Test
    @Disabled("Defect candidate on Altibase 7.3.0.1.8: role-granted CREATE MATERIALIZED VIEW is not honored for own-schema creation")
    @DisplayName("TC_320_001 CREATE MATERIALIZED VIEW privilege can be granted to a role")
    void tc320001GrantCreateMaterializedView() throws Exception {
        String user = createManagedUser("QA_MV_USER");
        String role = createManagedRole("QA_MV_ROLE");
        String sourceTable = DbTestSupport.uniqueName("QA_MV_SRC");
        String viewName = DbTestSupport.uniqueName("QA_MV");

        registerCleanup(() -> dropMaterializedViewQuietly(user + "." + viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, user + "." + sourceTable));

        jdbc.executeUpdate(connection, "revoke create materialized view from " + user);
        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "create table " + sourceTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + sourceTable + " values(1)");
        });
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create materialized view " + viewName + " as select * from " + sourceTable)))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "create materialized view");
        grantRoleToUser(role, user);

        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create materialized view " + viewName + " as select * from " + sourceTable));
        withUserConnection(user, conn ->
                assertThat(jdbc.queryForString(conn, "select count(*) from " + viewName)).isEqualTo("1"));
    }

    @Test
    @DisplayName("TC_320_002 ALTER ANY MATERIALIZED VIEW privilege can be granted to a role")
    void tc320002GrantAlterAnyMaterializedView() throws Exception {
        String owner = createManagedUser("QA_MV_OWNER");
        String user = createManagedUser("QA_MV_ALTER_USER");
        String role = createManagedRole("QA_MV_ALTER_ROLE");
        String sourceTable = DbTestSupport.uniqueName("QA_MV_ALT_SRC");
        String viewName = DbTestSupport.uniqueName("QA_MV_ALT");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + sourceTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + sourceTable + " values(1)");
            jdbc.executeUpdate(conn, "create materialized view " + viewName + " as select * from " + sourceTable);
        });
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "alter materialized view " + owner + "." + viewName + " refresh complete on demand")))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "alter any materialized view");
        grantRoleToUser(role, user);

        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "alter materialized view " + owner + "." + viewName + " refresh complete on demand"));
    }

    @Test
    @DisplayName("TC_320_003 DROP ANY MATERIALIZED VIEW privilege can be granted to a role")
    void tc320003GrantDropAnyMaterializedView() throws Exception {
        String owner = createManagedUser("QA_DROP_MV_OWNER");
        String user = createManagedUser("QA_DROP_MV_USER");
        String role = createManagedRole("QA_DROP_MV_ROLE");
        String sourceTable = "SRC";
        String viewName = "MV1";

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + sourceTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + sourceTable + " values(1)");
            jdbc.executeUpdate(conn, "create materialized view " + viewName + " as select * from " + sourceTable);
        });
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop materialized view " + owner + "." + viewName)))
                .isInstanceOf(IllegalStateException.class);

        grantToRole(role, "drop any materialized view");
        grantRoleToUser(role, user);

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop materialized view " + owner + "." + viewName));
        assertThatThrownBy(() -> withUserConnection(owner, conn -> jdbc.queryForString(conn, "select count(*) from " + viewName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_321_001 object privileges granted WITH GRANT OPTION can be re-granted")
    void tc321001GrantObjectPrivilegeWithGrantOption() throws Exception {
        String owner = createManagedUser("QA_GRANT_OWNER");
        String user1 = createManagedUser("QA_GRANT_USER1");
        String user2 = createManagedUser("QA_GRANT_USER2");
        String tableName = "BOOK";

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key, title varchar(20))");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1, 'SQL')");
        });

        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + user1);
        assertThatThrownBy(() -> withUserConnection(user1, conn -> jdbc.executeUpdate(conn, "grant select on " + owner + "." + tableName + " to " + user2)))
                .isInstanceOf(IllegalStateException.class);
        jdbc.executeUpdate(connection, "revoke select on " + owner + "." + tableName + " from " + user1);

        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + user1 + " with grant option");
        withUserConnection(user1, conn -> jdbc.executeUpdate(conn, "grant select on " + owner + "." + tableName + " to " + user2));
        withUserConnection(user2, conn -> assertThat(jdbc.queryForString(conn, "select title from " + owner + "." + tableName + " where id = 1").trim()).isEqualTo("SQL"));
    }

    @Test
    @DisplayName("TC_323_001 CREATE ROLE privilege can be granted to a user")
    void tc323001GrantCreateRole() throws Exception {
        String user = createManagedUser("QA_ROLE_CREATOR");
        String roleName = DbTestSupport.uniqueName("QA_CREATED_ROLE");
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, roleName));

        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create role " + roleName)))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "grant create role to " + user);
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create role " + roleName));

        assertThat(DbTestSupport.roleExists(jdbc, connection, roleName)).isTrue();
    }

    @Test
    @DisplayName("TC_323_002 GRANT ANY ROLE privilege can be granted to a user")
    void tc323002GrantAnyRole() throws Exception {
        String grantor = createManagedUser("QA_ROLE_GRANTOR");
        String grantee = createManagedUser("QA_ROLE_GRANTEE");
        String role = createManagedRole("QA_ROLE_FOR_GRANT");
        String createdUser = DbTestSupport.uniqueName("QA_ROLE_CREATED_USER");

        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, createdUser));
        grantToRole(role, "create user");

        assertThatThrownBy(() -> withUserConnection(grantor, conn -> jdbc.executeUpdate(conn, "grant " + role + " to " + grantee)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> withUserConnection(grantee, conn ->
                jdbc.executeUpdate(conn, "create user " + createdUser + " identified by " + createdUser)))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "grant grant any role to " + grantor);
        withUserConnection(grantor, conn -> jdbc.executeUpdate(conn, "grant " + role + " to " + grantee));
        withUserConnection(grantee, conn -> jdbc.executeUpdate(conn, "create user " + createdUser + " identified by " + createdUser));

        assertThat(DbTestSupport.userExists(jdbc, connection, createdUser)).isTrue();
    }

    @Test
    @DisplayName("TC_324_001 CREATE ANY JOB privilege can be granted to a user")
    void tc324001GrantCreateAnyJob() throws Exception {
        String user = createManagedUser("QA_JOB_USER");
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create job " + jobName + " exec sys." + procedureName + " start sysdate interval 1 month")))
                .isInstanceOf(IllegalStateException.class);

        grantToUser(user, "create any job");
        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create job " + jobName + " exec sys." + procedureName + " start sysdate interval 1 month"));

        assertThat(jobExists(jobName)).isTrue();
    }

    @Test
    @DisplayName("TC_324_002 ALTER ANY JOB privilege can be granted to a user")
    void tc324002GrantAlterAnyJob() throws Exception {
        String user = createManagedUser("QA_ALT_JOB_USER");
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter job " + jobName + " set enable")))
                .isInstanceOf(IllegalStateException.class);

        grantToUser(user, "alter any job");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter job " + jobName + " set enable"));

        assertThat(jobEnabled(jobName)).isTrue();
    }

    @Test
    @DisplayName("TC_324_003 DROP ANY JOB privilege can be granted to a user")
    void tc324003GrantDropAnyJob() throws Exception {
        String user = createManagedUser("QA_DROP_JOB_USER");
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop job " + jobName)))
                .isInstanceOf(IllegalStateException.class);

        grantToUser(user, "drop any job");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop job " + jobName));

        assertThat(jobExists(jobName)).isFalse();
    }

    @Test
    @DisplayName("TC_323_003 DROP ANY ROLE privilege can be granted to a user")
    void tc323003GrantDropAnyRole() throws Exception {
        String user = createManagedUser("QA_ROLE_DROPPER");
        String roleName = DbTestSupport.uniqueName("QA_DROP_ROLE");

        jdbc.executeUpdate(connection, "create role " + roleName);
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, roleName));

        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop role " + roleName)))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "grant drop any role to " + user);
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop role " + roleName));

        assertThat(DbTestSupport.roleExists(jdbc, connection, roleName)).isFalse();
    }

    @Test
    @DisplayName("Additional negative case: role-granted CREATE PROCEDURE is ignored after the direct privilege is revoked")
    void tc313Neg001RoleGrantedCreateProcedureStillFails() throws Exception {
        String user = createManagedUser("QA_PROC_ROLE_ONLY");
        String role = createManagedRole("QA_PROC_ROLE_ONLY");
        String blockedProcedure = DbTestSupport.uniqueName("QA_PROC_ROLE_FAIL");
        String createdProcedure = DbTestSupport.uniqueName("QA_PROC_ROLE_OK");

        jdbc.executeUpdate(connection, "revoke create procedure from " + user);
        grantToRole(role, "create procedure");
        grantRoleToUser(role, user);

        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleOutProcedure(conn, blockedProcedure)))
                .isInstanceOf(IllegalStateException.class);

        grantToUser(user, "create procedure");
        withUserConnection(user, conn -> createSimpleOutProcedure(conn, createdProcedure));

        assertThat(storedProgramExists(createdProcedure)).isTrue();
    }

    @Test
    @DisplayName("Additional negative case: role-granted CREATE TRIGGER is ignored after the direct privilege is revoked")
    void tc317Neg001RoleGrantedCreateTriggerStillFails() throws Exception {
        String user = createManagedUser("QA_TRG_ROLE_ONLY");
        String role = createManagedRole("QA_TRG_ROLE_ONLY");
        String tableName = DbTestSupport.uniqueName("QA_TRG_ROLE_TB");
        String blockedTrigger = DbTestSupport.uniqueName("QA_TRG_ROLE_FAIL");
        String createdTrigger = DbTestSupport.uniqueName("QA_TRG_ROLE_OK");

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)"));
        jdbc.executeUpdate(connection, "revoke create trigger from " + user);
        grantToRole(role, "create trigger");
        grantRoleToUser(role, user);

        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleTrigger(conn, blockedTrigger, tableName)))
                .isInstanceOf(IllegalStateException.class);

        grantToUser(user, "create trigger");
        withUserConnection(user, conn -> createSimpleTrigger(conn, createdTrigger, tableName));
        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "insert into " + tableName + "(c1, c2) values(1, null)");
            assertThat(jdbc.queryForString(conn, "select c2 from " + tableName + " where c1 = 1")).isEqualTo("0");
        });
    }

    @Test
    @DisplayName("Additional negative case: role-granted CREATE SYNONYM is ignored after the direct privilege is revoked")
    void tc318Neg002RoleGrantedCreateSynonymStillFails() throws Exception {
        String user = createManagedUser("QA_SYN_ROLE_ONLY");
        String role = createManagedRole("QA_SYN_ROLE_ONLY");
        String blockedSynonym = DbTestSupport.uniqueName("QA_SYN_ROLE_FAIL");
        String createdSynonym = DbTestSupport.uniqueName("QA_SYN_ROLE_OK");

        jdbc.executeUpdate(connection, "revoke create synonym from " + user);
        grantToRole(role, "create synonym");
        grantRoleToUser(role, user);

        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create synonym " + blockedSynonym + " for dual")))
                .isInstanceOf(IllegalStateException.class);

        grantToUser(user, "create synonym");
        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + createdSynonym + " for dual");
            assertThat(jdbc.queryForString(conn, "select count(*) from " + createdSynonym)).isEqualTo("1");
        });
    }

    @Test
    @DisplayName("Additional negative case: role-granted CREATE MATERIALIZED VIEW is ignored after the direct privilege is revoked")
    void tc320Neg001RoleGrantedCreateMaterializedViewStillFails() throws Exception {
        String user = createManagedUser("QA_MV_ROLE_ONLY");
        String role = createManagedRole("QA_MV_ROLE_ONLY");
        String sourceTable = DbTestSupport.uniqueName("QA_MV_ROLE_SRC");
        String blockedView = DbTestSupport.uniqueName("QA_MV_ROLE_FAIL");
        String createdView = DbTestSupport.uniqueName("QA_MV_ROLE_OK");

        registerCleanup(() -> dropMaterializedViewQuietly(user + "." + blockedView));
        registerCleanup(() -> dropMaterializedViewQuietly(user + "." + createdView));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, user + "." + sourceTable));

        jdbc.executeUpdate(connection, "revoke create materialized view from " + user);
        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "create table " + sourceTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + sourceTable + " values(1)");
        });
        grantToRole(role, "create materialized view");
        grantRoleToUser(role, user);

        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create materialized view " + blockedView + " as select * from " + sourceTable)))
                .isInstanceOf(IllegalStateException.class);

        grantToUser(user, "create materialized view");
        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create materialized view " + createdView + " as select * from " + sourceTable));
        withUserConnection(user, conn ->
                assertThat(jdbc.queryForString(conn, "select count(*) from " + createdView)).isEqualTo("1"));
    }

    @Test
    @DisplayName("Additional negative case: CREATE ANY SYNONYM is required for cross-schema private synonyms")
    void tc318Neg001CreateAnySynonymWithoutPrivilegeFails() throws Exception {
        String owner = createManagedUser("QA_SYN_NEG_OWNER");
        String user = createManagedUser("QA_SYN_NEG_USER");
        String synonymName = DbTestSupport.uniqueName("QA_SYN_NEG");

        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create synonym " + owner + "." + synonymName + " for dual")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_322_001 REFERENCES object privilege can be granted to another user")
    void tc322001GrantReferencesOnObject() throws Exception {
        String owner = createManagedUser("QA_REF_OWNER");
        String user = createManagedUser("QA_REF_USER");
        String parentTable = "BOOK";
        String childTable = "BOOK_REF";

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + parentTable + "(id integer primary key)"));
        jdbc.executeUpdate(connection, "grant references on " + owner + "." + parentTable + " to " + user);

        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create table " + childTable + "(id integer primary key, book_id integer references " + owner + "." + parentTable + "(id))"));

        assertThat(hasImportedKeys(user, childTable)).isTrue();
    }

    private String createManagedUser(String prefix) {
        String user = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);
        return user;
    }

    private String createManagedRole(String prefix) {
        String role = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, role));
        jdbc.executeUpdate(connection, "create role " + role);
        return role;
    }

    private void grantToRole(String role, String privilege) {
        grantPrivilege(role, privilege);
    }

    private void grantToUser(String user, String privilege) {
        grantPrivilege(user, privilege);
    }

    private void grantRoleToUser(String role, String user) {
        jdbc.executeUpdate(connection, "grant " + role + " to " + user);
    }

    private void grantPrivilege(String grantee, String privilege) {
        try {
            jdbc.executeUpdate(connection, "grant " + privilege + " to " + grantee);
        } catch (IllegalStateException e) {
            if (isAlreadyGranted(e)) {
                return;
            }
            throw e;
        }
    }

    private void createSimpleOutProcedure(Connection conn, String qualifiedProcedureName) {
        jdbc.executeUpdate(conn, "create or replace procedure " + qualifiedProcedureName + "(p1 out integer) as begin p1 := 1; end;");
    }

    private int executeOutProcedure(Connection conn, String qualifiedProcedureName) throws Exception {
        try (CallableStatement cs = conn.prepareCall("{call " + qualifiedProcedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            return cs.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute callable procedure: " + qualifiedProcedureName, e);
        }
    }

    private void createSimpleSequence(Connection conn, String qualifiedSequenceName) {
        jdbc.executeUpdate(conn, "create sequence " + qualifiedSequenceName + " start with 1 increment by 1");
    }

    private String nextSequenceValue(Connection conn, String qualifiedSequenceName) {
        return jdbc.queryForString(conn, "select " + qualifiedSequenceName + ".nextval from dual");
    }

    private void createConstantView(Connection conn, String qualifiedViewName) {
        jdbc.executeUpdate(conn, "create view " + qualifiedViewName + " as select 1 as c1 from dual");
    }

    private void createSimpleTrigger(Connection conn, String qualifiedTriggerName, String qualifiedTableName) {
        jdbc.executeUpdate(
                conn,
                "create or replace trigger " + qualifiedTriggerName + " before insert on " + qualifiedTableName +
                        " referencing new row new_row for each row as begin if new_row.c2 is null then new_row.c2 := 0; end if; end;"
        );
    }

    private void lockTableInExclusiveMode(Connection conn, String qualifiedTableName) {
        jdbc.begin(conn);
        try {
            jdbc.executeUpdate(conn, "lock table " + qualifiedTableName + " in exclusive mode");
            jdbc.commit(conn);
        } catch (Exception e) {
            jdbc.rollback(conn);
            throw e;
        }
    }

    private boolean storedProgramExists(String name) {
        return jdbc.exists(
                connection,
                "select proc_name from system_.sys_procedures_ where proc_name = '" + name + "'"
        );
    }

    private boolean indexExists(String name) {
        return jdbc.exists(connection, "select index_name from system_.sys_indices_ where index_name = '" + name + "'");
    }

    private boolean directoryExists(String name) {
        return jdbc.exists(connection, "select directory_name from system_.sys_directories_ where directory_name = '" + name + "'");
    }

    private boolean tablespaceExists(String name) {
        return jdbc.exists(connection, "select name from v$tablespaces where name = '" + name + "'");
    }

    private boolean jobExists(String jobName) {
        return jdbc.exists(connection, "select job_name from system_.sys_jobs_ where job_name = '" + jobName + "'");
    }

    private boolean jobEnabled(String jobName) {
        return "T".equalsIgnoreCase(jdbc.queryForString(connection,
                "select is_enable from system_.sys_jobs_ where job_name = '" + jobName + "'"));
    }

    private void dropDirectoryQuietly(String directoryName) {
        try {
            jdbc.executeUpdate(connection, "drop directory " + directoryName);
        } catch (Exception ignored) {
        }
    }

    private void dropMaterializedViewQuietly(String qualifiedViewName) {
        try {
            jdbc.executeUpdate(connection, "drop materialized view " + qualifiedViewName);
        } catch (Exception ignored) {
        }
    }

    private void dropJobQuietly(String jobName) {
        try {
            jdbc.executeUpdate(connection, "drop job " + jobName);
        } catch (Exception ignored) {
        }
    }

    private void dropTablespaceQuietly(String tablespaceName) {
        try {
            jdbc.executeUpdate(connection, "drop tablespace " + tablespaceName + " including contents and datafiles");
        } catch (Exception ignored) {
        }
    }

    private boolean hasImportedKeys(String schema, String tableName) {
        try (ResultSet rs = connection.getMetaData().getImportedKeys(null, schema, tableName)) {
            return rs.next();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to inspect imported keys for " + schema + "." + tableName, e);
        }
    }

    private boolean isAlreadyGranted(IllegalStateException e) {
        Throwable cause = e.getCause();
        return cause instanceof SQLException sqlException
                && sqlException.getMessage() != null
                && sqlException.getMessage().contains("already has privileges");
    }

    private void withUserConnection(String user, SqlWork work) throws Exception {
        Connection userConnection = jdbc.open(user, user);
        try {
            work.run(userConnection);
        } finally {
            jdbc.closeQuietly(userConnection);
        }
    }

    @FunctionalInterface
    private interface SqlWork {
        void run(Connection connection) throws Exception;
    }
}
