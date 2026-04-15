package com.altibase.qa.security;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrivilegeRevokeJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_325_001 revoking CREATE USER blocks user creation")
    void tc325001RevokeCreateUser() throws Exception {
        String user = createManagedUser("QA_CREATE_USER");
        String createdUser = DbTestSupport.uniqueName("QA_CREATED_USER");
        String blockedUser = DbTestSupport.uniqueName("QA_BLOCKED_USER");

        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, createdUser));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, blockedUser));

        grant(user, "create user");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create user " + createdUser + " identified by " + createdUser));
        assertThat(DbTestSupport.userExists(jdbc, connection, createdUser)).isTrue();

        revoke("create user", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create user " + blockedUser + " identified by " + blockedUser)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_325_002 revoking ALTER USER blocks account changes")
    void tc325002RevokeAlterUser() throws Exception {
        String user = createManagedUser("QA_ALTER_USER");
        String lockedTarget = createManagedUser("QA_LOCK_TARGET");
        String blockedTarget = createManagedUser("QA_BLOCK_TARGET");

        grant(user, "alter user");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter user " + lockedTarget + " account lock"));
        assertThatThrownBy(() -> jdbc.open(lockedTarget, lockedTarget))
                .isInstanceOf(IllegalStateException.class);

        revoke("alter user", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter user " + blockedTarget + " account lock")))
                .isInstanceOf(IllegalStateException.class);
        jdbc.closeQuietly(jdbc.open(blockedTarget, blockedTarget));
    }

    @Test
    @DisplayName("TC_325_003 revoking DROP USER blocks user deletion")
    void tc325003RevokeDropUser() throws Exception {
        String user = createManagedUser("QA_DROP_USER");
        String droppableUser = createManagedUser("QA_DROP_TARGET_OK");
        String blockedUser = createManagedUser("QA_DROP_TARGET_BLOCK");

        grant(user, "drop user");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop user " + droppableUser));
        assertThat(DbTestSupport.userExists(jdbc, connection, droppableUser)).isFalse();

        revoke("drop user", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop user " + blockedUser)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_326_001 revoking CREATE TABLE blocks table creation in own schema")
    void tc326001RevokeCreateTable() throws Exception {
        String user = createManagedUser("QA_TABLE_USER");
        String createdTable = DbTestSupport.uniqueName("QA_TB_OK");
        String blockedTable = DbTestSupport.uniqueName("QA_TB_BLOCK");

        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, user + "." + createdTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, user + "." + blockedTable));

        grant(user, "create table");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create table " + createdTable + "(c1 integer)"));
        assertThat(DbTestSupport.tableExists(connection, user, createdTable)).isTrue();

        revoke("create table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create table " + blockedTable + "(c1 integer)")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_326_002 revoking CREATE ANY TABLE blocks cross-schema table creation")
    void tc326002RevokeCreateAnyTable() throws Exception {
        String owner = createManagedUser("QA_ANY_TB_OWNER");
        String user = createManagedUser("QA_ANY_TB_USER");
        String createdTable = DbTestSupport.uniqueName("QA_ANY_TB_OK");
        String blockedTable = DbTestSupport.uniqueName("QA_ANY_TB_BLOCK");

        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, owner + "." + createdTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, owner + "." + blockedTable));

        grant(user, "create any table");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create table " + owner + "." + createdTable + "(c1 integer)"));
        assertThat(DbTestSupport.tableExists(connection, owner, createdTable)).isTrue();

        revoke("create any table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create table " + owner + "." + blockedTable + "(c1 integer)")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_326_003 revoking ALTER ANY TABLE blocks table definition changes")
    void tc326003RevokeAlterAnyTable() throws Exception {
        String owner = createManagedUser("QA_ALT_TB_OWNER");
        String user = createManagedUser("QA_ALT_TB_USER");
        String alteredTable = DbTestSupport.uniqueName("QA_ALT_TB_OK");
        String blockedTable = DbTestSupport.uniqueName("QA_ALT_TB_BLOCK");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + alteredTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "create table " + blockedTable + "(c1 integer)");
        });

        grant(user, "alter any table");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter table " + owner + "." + alteredTable + " add column (c2 integer)"));
        assertThat(DbTestSupport.columnExists(connection, owner, alteredTable, "c2")).isTrue();

        revoke("alter any table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter table " + owner + "." + blockedTable + " add column (c2 integer)")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_326_004 revoking DROP ANY TABLE blocks table deletion")
    void tc326004RevokeDropAnyTable() throws Exception {
        String owner = createManagedUser("QA_DROP_TB_OWNER");
        String user = createManagedUser("QA_DROP_TB_USER");
        String droppableTable = DbTestSupport.uniqueName("QA_DROP_TB_OK");
        String blockedTable = DbTestSupport.uniqueName("QA_DROP_TB_BLOCK");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + droppableTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "create table " + blockedTable + "(c1 integer)");
        });

        grant(user, "drop any table");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop table " + owner + "." + droppableTable + " cascade"));
        assertThat(DbTestSupport.tableExists(connection, owner, droppableTable)).isFalse();

        revoke("drop any table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop table " + owner + "." + blockedTable + " cascade")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_326_005 revoking SELECT ANY TABLE blocks table queries")
    void tc326005RevokeSelectAnyTable() throws Exception {
        String owner = createManagedUser("QA_SEL_TB_OWNER");
        String user = createManagedUser("QA_SEL_TB_USER");
        String tableName = DbTestSupport.uniqueName("QA_SEL_TB");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1)");
        });

        grant(user, "select any table");
        withUserConnection(user, conn -> assertThat(jdbc.queryForString(conn, "select c1 from " + owner + "." + tableName)).isEqualTo("1"));

        revoke("select any table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.queryForString(conn, "select c1 from " + owner + "." + tableName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_326_006 revoking INSERT ANY TABLE blocks data insertion")
    void tc326006RevokeInsertAnyTable() throws Exception {
        String owner = createManagedUser("QA_INS_TB_OWNER");
        String user = createManagedUser("QA_INS_TB_USER");
        String tableName = DbTestSupport.uniqueName("QA_INS_TB");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer)"));

        grant(user, "insert any table");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "insert into " + owner + "." + tableName + " values(1)"));
        assertThat(jdbc.queryForString(connection, "select count(*) from " + owner + "." + tableName)).isEqualTo("1");

        revoke("insert any table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "insert into " + owner + "." + tableName + " values(2)")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_326_007 revoking DELETE ANY TABLE blocks data deletion")
    void tc326007RevokeDeleteAnyTable() throws Exception {
        String owner = createManagedUser("QA_DEL_TB_OWNER");
        String user = createManagedUser("QA_DEL_TB_USER");
        String tableName = DbTestSupport.uniqueName("QA_DEL_TB");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(2)");
        });

        grant(user, "delete any table");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "delete from " + owner + "." + tableName + " where c1 = 1"));
        assertThat(jdbc.queryForString(connection, "select count(*) from " + owner + "." + tableName)).isEqualTo("1");

        revoke("delete any table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "delete from " + owner + "." + tableName + " where c1 = 2")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_326_008 revoking UPDATE ANY TABLE blocks data updates")
    void tc326008RevokeUpdateAnyTable() throws Exception {
        String owner = createManagedUser("QA_UPD_TB_OWNER");
        String user = createManagedUser("QA_UPD_TB_USER");
        String tableName = DbTestSupport.uniqueName("QA_UPD_TB");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer primary key, c2 integer)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1, 10)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(2, 20)");
        });

        grant(user, "update any table");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "update " + owner + "." + tableName + " set c2 = 11 where c1 = 1"));
        assertThat(jdbc.queryForString(connection, "select c2 from " + owner + "." + tableName + " where c1 = 1")).isEqualTo("11");

        revoke("update any table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "update " + owner + "." + tableName + " set c2 = 21 where c1 = 2")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForString(connection, "select c2 from " + owner + "." + tableName + " where c1 = 2")).isEqualTo("20");
    }

    @Test
    @DisplayName("TC_326_009 revoking LOCK ANY TABLE blocks table locks")
    void tc326009RevokeLockAnyTable() throws Exception {
        String owner = createManagedUser("QA_LOCK_TB_OWNER");
        String user = createManagedUser("QA_LOCK_TB_USER");
        String tableName = DbTestSupport.uniqueName("QA_LOCK_TB");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer)"));

        grant(user, "lock any table");
        withUserConnection(user, conn -> lockTableInExclusiveMode(conn, owner + "." + tableName));

        revoke("lock any table", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> lockTableInExclusiveMode(conn, owner + "." + tableName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_327_001 revoking CREATE ANY INDEX blocks index creation")
    void tc327001RevokeCreateAnyIndex() throws Exception {
        String owner = createManagedUser("QA_IDX_OWNER");
        String user = createManagedUser("QA_IDX_USER");
        String tableName = DbTestSupport.uniqueName("QA_IDX_TB");
        String createdIndex = DbTestSupport.uniqueName("QA_IDX_OK");
        String blockedIndex = DbTestSupport.uniqueName("QA_IDX_BLOCK");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)"));

        grant(user, "create any index");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create index " + owner + "." + createdIndex + " on " + owner + "." + tableName + "(c1)"));
        assertThat(indexExists(createdIndex)).isTrue();

        revoke("create any index", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create index " + owner + "." + blockedIndex + " on " + owner + "." + tableName + "(c2)")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_327_002 revoking ALTER ANY INDEX blocks index definition changes")
    void tc327002RevokeAlterAnyIndex() throws Exception {
        String owner = createManagedUser("QA_ALT_IDX_OWNER");
        String user = createManagedUser("QA_ALT_IDX_USER");
        String tableName = DbTestSupport.uniqueName("QA_ALT_IDX_TB");
        String alteredIndex = DbTestSupport.uniqueName("QA_ALT_IDX_OK");
        String blockedIndex = DbTestSupport.uniqueName("QA_ALT_IDX_BLOCK");
        String renamedIndex = DbTestSupport.uniqueName("QA_ALT_IDX_NEW");
        String blockedRename = DbTestSupport.uniqueName("QA_ALT_IDX_BLOCK_NEW");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)");
            jdbc.executeUpdate(conn, "create index " + alteredIndex + " on " + tableName + "(c1)");
            jdbc.executeUpdate(conn, "create index " + blockedIndex + " on " + tableName + "(c2)");
        });

        grant(user, "alter any index");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter index " + owner + "." + alteredIndex + " rename to " + renamedIndex));
        assertThat(indexExists(renamedIndex)).isTrue();

        revoke("alter any index", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter index " + owner + "." + blockedIndex + " rename to " + blockedRename)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_327_003 revoking DROP ANY INDEX blocks index deletion")
    void tc327003RevokeDropAnyIndex() throws Exception {
        String owner = createManagedUser("QA_DROP_IDX_OWNER");
        String user = createManagedUser("QA_DROP_IDX_USER");
        String tableName = DbTestSupport.uniqueName("QA_DROP_IDX_TB");
        String droppableIndex = DbTestSupport.uniqueName("QA_DROP_IDX_OK");
        String blockedIndex = DbTestSupport.uniqueName("QA_DROP_IDX_BLOCK");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)");
            jdbc.executeUpdate(conn, "create index " + droppableIndex + " on " + tableName + "(c1)");
            jdbc.executeUpdate(conn, "create index " + blockedIndex + " on " + tableName + "(c2)");
        });

        grant(user, "drop any index");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop index " + owner + "." + droppableIndex));
        assertThat(indexExists(droppableIndex)).isFalse();

        revoke("drop any index", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop index " + owner + "." + blockedIndex)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_328_001 revoking CREATE PROCEDURE blocks procedure creation in own schema")
    void tc328001RevokeCreateProcedure() throws Exception {
        String user = createManagedUser("QA_PROC_USER");
        String createdProcedure = DbTestSupport.uniqueName("QA_PROC_OK");
        String blockedProcedure = DbTestSupport.uniqueName("QA_PROC_BLOCK");

        grant(user, "create procedure");
        withUserConnection(user, conn -> createSimpleOutProcedure(conn, createdProcedure));
        assertThat(storedProgramExists(createdProcedure)).isTrue();

        revoke("create procedure", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleOutProcedure(conn, blockedProcedure)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_328_002 revoking CREATE ANY PROCEDURE blocks procedure creation in other schemas")
    void tc328002RevokeCreateAnyProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_OWNER");
        String user = createManagedUser("QA_PROC_ANY");
        String createdProcedure = DbTestSupport.uniqueName("QA_ANY_PROC_OK");
        String blockedProcedure = DbTestSupport.uniqueName("QA_ANY_PROC_BLOCK");

        grant(user, "create any procedure");
        withUserConnection(user, conn -> createSimpleOutProcedure(conn, owner + "." + createdProcedure));
        assertThat(storedProgramExists(createdProcedure)).isTrue();

        revoke("create any procedure", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleOutProcedure(conn, owner + "." + blockedProcedure)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_328_003 revoking ALTER ANY PROCEDURE blocks procedure recompilation")
    void tc328003RevokeAlterAnyProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_OWNER");
        String user = createManagedUser("QA_PROC_ALTER");
        String procedureName = DbTestSupport.uniqueName("QA_ALT_PROC");

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));

        grant(user, "alter any procedure");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter procedure " + owner + "." + procedureName + " compile"));

        revoke("alter any procedure", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter procedure " + owner + "." + procedureName + " compile")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_328_004 revoking DROP ANY PROCEDURE blocks procedure deletion")
    void tc328004RevokeDropAnyProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_OWNER");
        String user = createManagedUser("QA_PROC_DROP");
        String droppableProcedure = DbTestSupport.uniqueName("QA_DROP_PROC_OK");
        String blockedProcedure = DbTestSupport.uniqueName("QA_DROP_PROC_BLOCK");

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, droppableProcedure));
        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, blockedProcedure));

        grant(user, "drop any procedure");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop procedure " + owner + "." + droppableProcedure));
        assertThat(storedProgramExists(droppableProcedure)).isFalse();

        revoke("drop any procedure", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop procedure " + owner + "." + blockedProcedure)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_328_005 revoking EXECUTE ANY PROCEDURE blocks procedure execution")
    void tc328005RevokeExecuteAnyProcedure() throws Exception {
        String owner = createManagedUser("QA_PROC_OWNER");
        String user = createManagedUser("QA_PROC_EXEC");
        String procedureName = DbTestSupport.uniqueName("QA_EXEC_PROC");

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));

        grant(user, "execute any procedure");
        withUserConnection(user, conn -> assertThat(executeOutProcedure(conn, owner + "." + procedureName)).isEqualTo(1));

        revoke("execute any procedure", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> executeOutProcedure(conn, owner + "." + procedureName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_329_001 revoking CREATE SEQUENCE blocks sequence creation in own schema")
    void tc329001RevokeCreateSequence() throws Exception {
        String user = createManagedUser("QA_SEQ_USER");
        String createdSequence = DbTestSupport.uniqueName("QA_SEQ_OK");
        String blockedSequence = DbTestSupport.uniqueName("QA_SEQ_BLOCK");

        grant(user, "create sequence");
        withUserConnection(user, conn -> createSimpleSequence(conn, createdSequence));
        withUserConnection(user, conn -> assertThat(nextSequenceValue(conn, createdSequence)).isEqualTo("1"));

        revoke("create sequence", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleSequence(conn, blockedSequence)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_329_002 revoking CREATE ANY SEQUENCE blocks sequence creation in other schemas")
    void tc329002RevokeCreateAnySequence() throws Exception {
        String owner = createManagedUser("QA_SEQ_OWNER");
        String user = createManagedUser("QA_SEQ_ANY");
        String createdSequence = DbTestSupport.uniqueName("QA_SEQ_ANY_OK");
        String blockedSequence = DbTestSupport.uniqueName("QA_SEQ_ANY_BLOCK");

        grant(user, "create any sequence");
        withUserConnection(user, conn -> createSimpleSequence(conn, owner + "." + createdSequence));
        assertThat(nextSequenceValue(connection, owner + "." + createdSequence)).isEqualTo("1");

        revoke("create any sequence", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleSequence(conn, owner + "." + blockedSequence)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_329_003 revoking ALTER ANY SEQUENCE blocks sequence changes")
    void tc329003RevokeAlterAnySequence() throws Exception {
        String owner = createManagedUser("QA_SEQ_OWNER");
        String user = createManagedUser("QA_SEQ_ALTER");
        String sequenceName = DbTestSupport.uniqueName("QA_ALT_SEQ");

        withUserConnection(owner, conn -> createSimpleSequence(conn, sequenceName));

        grant(user, "alter any sequence");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter sequence " + owner + "." + sequenceName + " increment by 5"));

        revoke("alter any sequence", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter sequence " + owner + "." + sequenceName + " increment by 3")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_329_004 revoking DROP ANY SEQUENCE blocks sequence deletion")
    void tc329004RevokeDropAnySequence() throws Exception {
        String owner = createManagedUser("QA_SEQ_OWNER");
        String user = createManagedUser("QA_SEQ_DROP");
        String droppableSequence = DbTestSupport.uniqueName("QA_DROP_SEQ_OK");
        String blockedSequence = DbTestSupport.uniqueName("QA_DROP_SEQ_BLOCK");

        withUserConnection(owner, conn -> createSimpleSequence(conn, droppableSequence));
        withUserConnection(owner, conn -> createSimpleSequence(conn, blockedSequence));

        grant(user, "drop any sequence");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop sequence " + owner + "." + droppableSequence));
        assertThatThrownBy(() -> jdbc.queryForString(connection, "select " + owner + "." + droppableSequence + ".nextval from dual"))
                .isInstanceOf(IllegalStateException.class);

        revoke("drop any sequence", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop sequence " + owner + "." + blockedSequence)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_329_005 revoking SELECT ANY SEQUENCE blocks sequence access")
    void tc329005RevokeSelectAnySequence() throws Exception {
        String owner = createManagedUser("QA_SEQ_OWNER");
        String user = createManagedUser("QA_SEQ_SELECT");
        String sequenceName = DbTestSupport.uniqueName("QA_SELECT_SEQ");

        withUserConnection(owner, conn -> createSimpleSequence(conn, sequenceName));

        grant(user, "select any sequence");
        withUserConnection(user, conn -> assertThat(nextSequenceValue(conn, owner + "." + sequenceName)).isEqualTo("1"));

        revoke("select any sequence", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> nextSequenceValue(conn, owner + "." + sequenceName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_330_001 revoking CREATE VIEW blocks view creation in own schema")
    void tc330001RevokeCreateView() throws Exception {
        String user = createManagedUser("QA_VIEW_USER");
        String createdView = DbTestSupport.uniqueName("QA_VIEW_OK");
        String blockedView = DbTestSupport.uniqueName("QA_VIEW_BLOCK");

        grant(user, "create view");
        withUserConnection(user, conn -> createConstantView(conn, createdView));
        withUserConnection(user, conn -> assertThat(jdbc.queryForString(conn, "select c1 from " + createdView)).isEqualTo("1"));

        revoke("create view", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createConstantView(conn, blockedView)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_330_002 revoking CREATE ANY VIEW blocks view creation in other schemas")
    void tc330002RevokeCreateAnyView() throws Exception {
        String owner = createManagedUser("QA_VIEW_OWNER");
        String user = createManagedUser("QA_VIEW_ANY");
        String createdView = DbTestSupport.uniqueName("QA_VIEW_ANY_OK");
        String blockedView = DbTestSupport.uniqueName("QA_VIEW_ANY_BLOCK");

        grant(user, "create any view");
        withUserConnection(user, conn -> createConstantView(conn, owner + "." + createdView));
        assertThat(jdbc.queryForString(connection, "select c1 from " + owner + "." + createdView)).isEqualTo("1");

        revoke("create any view", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createConstantView(conn, owner + "." + blockedView)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_330_003 revoking DROP ANY VIEW blocks view deletion")
    void tc330003RevokeDropAnyView() throws Exception {
        String owner = createManagedUser("QA_VIEW_OWNER");
        String user = createManagedUser("QA_VIEW_DROP");
        String droppableView = DbTestSupport.uniqueName("QA_DROP_VIEW_OK");
        String blockedView = DbTestSupport.uniqueName("QA_DROP_VIEW_BLOCK");

        withUserConnection(owner, conn -> createConstantView(conn, droppableView));
        withUserConnection(owner, conn -> createConstantView(conn, blockedView));

        grant(user, "drop any view");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop view " + owner + "." + droppableView));
        assertThat(DbTestSupport.viewExists(connection, owner, droppableView)).isFalse();

        revoke("drop any view", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop view " + owner + "." + blockedView)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_331_001 revoking CREATE TABLESPACE blocks tablespace creation")
    void tc331001RevokeCreateTablespace() throws Exception {
        String user = createManagedUser("QA_TS_CREATE_USER");
        String createdTablespace = DbTestSupport.uniqueName("QA_TS_OK");
        String blockedTablespace = DbTestSupport.uniqueName("QA_TS_BLOCK");

        registerCleanup(() -> dropTablespaceQuietly(createdTablespace));
        registerCleanup(() -> dropTablespaceQuietly(blockedTablespace));

        grant(user, "create tablespace");
        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create memory tablespace " + createdTablespace + " size 8M"));
        assertThat(tablespaceExists(createdTablespace)).isTrue();

        revoke("create tablespace", user);
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create memory tablespace " + blockedTablespace + " size 8M")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_331_002 revoking ALTER TABLESPACE blocks tablespace changes")
    void tc331002RevokeAlterTablespace() throws Exception {
        String user = createManagedUser("QA_TS_ALTER_USER");
        String tablespaceName = DbTestSupport.uniqueName("QA_TS_ALTER");

        registerCleanup(() -> dropTablespaceQuietly(tablespaceName));
        jdbc.executeUpdate(connection, "create memory tablespace " + tablespaceName + " size 8M");

        grant(user, "alter tablespace");
        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "alter tablespace " + tablespaceName + " alter autoextend on next 4M maxsize 20M"));

        revoke("alter tablespace", user);
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "alter tablespace " + tablespaceName + " alter autoextend on next 8M maxsize 24M")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_331_003 revoking DROP TABLESPACE blocks tablespace deletion")
    void tc331003RevokeDropTablespace() throws Exception {
        String user = createManagedUser("QA_TS_DROP_USER");
        String droppableTablespace = DbTestSupport.uniqueName("QA_TS_OK");
        String blockedTablespace = DbTestSupport.uniqueName("QA_TS_BLOCK");

        registerCleanup(() -> dropTablespaceQuietly(blockedTablespace));
        jdbc.executeUpdate(connection, "create memory tablespace " + droppableTablespace + " size 8M");
        jdbc.executeUpdate(connection, "create memory tablespace " + blockedTablespace + " size 8M");

        grant(user, "drop tablespace");
        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "drop tablespace " + droppableTablespace + " including contents and datafiles"));
        assertThat(tablespaceExists(droppableTablespace)).isFalse();

        revoke("drop tablespace", user);
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "drop tablespace " + blockedTablespace + " including contents and datafiles")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_332_001 revoking CREATE TRIGGER blocks trigger creation in own schema")
    void tc332001RevokeCreateTrigger() throws Exception {
        String user = createManagedUser("QA_TRG_USER");
        String tableName = DbTestSupport.uniqueName("QA_TRG_TB");
        String createdTrigger = DbTestSupport.uniqueName("QA_TRG_OK");
        String blockedTrigger = DbTestSupport.uniqueName("QA_TRG_BLOCK");

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)"));
        grant(user, "create trigger");
        withUserConnection(user, conn -> createSimpleTrigger(conn, createdTrigger, tableName));
        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "insert into " + tableName + "(c1, c2) values(1, null)");
            assertThat(jdbc.queryForString(conn, "select c2 from " + tableName + " where c1 = 1")).isEqualTo("0");
        });

        revoke("create trigger", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleTrigger(conn, blockedTrigger, tableName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_332_002 revoking CREATE ANY TRIGGER blocks trigger creation in other schemas")
    void tc332002RevokeCreateAnyTrigger() throws Exception {
        String owner = createManagedUser("QA_TRG_OWNER");
        String user = createManagedUser("QA_TRG_ANY");
        String tableName = DbTestSupport.uniqueName("QA_TRG_TB");
        String createdTrigger = DbTestSupport.uniqueName("QA_ANY_TRG_OK");
        String blockedTrigger = DbTestSupport.uniqueName("QA_ANY_TRG_BLOCK");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)"));
        grant(user, "create any trigger");
        withUserConnection(user, conn -> createSimpleTrigger(conn, owner + "." + createdTrigger, owner + "." + tableName));
        jdbc.executeUpdate(connection, "insert into " + owner + "." + tableName + "(c1, c2) values(1, null)");
        assertThat(jdbc.queryForString(connection, "select c2 from " + owner + "." + tableName + " where c1 = 1")).isEqualTo("0");

        revoke("create any trigger", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> createSimpleTrigger(conn, owner + "." + blockedTrigger, owner + "." + tableName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_332_003 revoking ALTER ANY TRIGGER blocks trigger changes")
    void tc332003RevokeAlterAnyTrigger() throws Exception {
        String owner = createManagedUser("QA_TRG_OWNER");
        String user = createManagedUser("QA_TRG_ALTER");
        String tableName = DbTestSupport.uniqueName("QA_TRG_TB");
        String triggerName = DbTestSupport.uniqueName("QA_ALT_TRG");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(c1 integer, c2 integer)");
            createSimpleTrigger(conn, triggerName, tableName);
        });

        grant(user, "alter any trigger");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter trigger " + owner + "." + triggerName + " disable"));
        jdbc.executeUpdate(connection, "insert into " + owner + "." + tableName + "(c1, c2) values(1, null)");
        assertThat(jdbc.queryForString(connection, "select c2 from " + owner + "." + tableName + " where c1 = 1")).isNull();

        revoke("alter any trigger", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter trigger " + owner + "." + triggerName + " enable")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_332_004 revoking DROP ANY TRIGGER blocks trigger deletion")
    void tc332004RevokeDropAnyTrigger() throws Exception {
        String owner = createManagedUser("QA_TRG_OWNER");
        String user = createManagedUser("QA_TRG_DROP");
        String droppableTable = DbTestSupport.uniqueName("QA_TRG_DROP_TB1");
        String blockedTable = DbTestSupport.uniqueName("QA_TRG_DROP_TB2");
        String droppableTrigger = DbTestSupport.uniqueName("QA_DROP_TRG_OK");
        String blockedTrigger = DbTestSupport.uniqueName("QA_DROP_TRG_BLOCK");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + droppableTable + "(c1 integer, c2 integer)");
            jdbc.executeUpdate(conn, "create table " + blockedTable + "(c1 integer, c2 integer)");
            createSimpleTrigger(conn, droppableTrigger, droppableTable);
            createSimpleTrigger(conn, blockedTrigger, blockedTable);
        });

        grant(user, "drop any trigger");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop trigger " + owner + "." + droppableTrigger));
        jdbc.executeUpdate(connection, "insert into " + owner + "." + droppableTable + "(c1, c2) values(1, null)");
        assertThat(jdbc.queryForString(connection, "select c2 from " + owner + "." + droppableTable + " where c1 = 1")).isNull();

        revoke("drop any trigger", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop trigger " + owner + "." + blockedTrigger)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_333_001 revoking CREATE PUBLIC SYNONYM blocks public synonym creation")
    void tc333001RevokeCreatePublicSynonym() throws Exception {
        String user = createManagedUser("QA_PUB_SYN");
        String createdSynonym = DbTestSupport.uniqueName("QA_PUB_SYN_OK");
        String blockedSynonym = DbTestSupport.uniqueName("QA_PUB_SYN_BLOCK");
        registerCleanup(() -> DbTestSupport.dropPublicSynonymQuietly(jdbc, connection, createdSynonym));
        registerCleanup(() -> DbTestSupport.dropPublicSynonymQuietly(jdbc, connection, blockedSynonym));

        grant(user, "create public synonym");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create public synonym " + createdSynonym + " for dual"));
        assertThat(jdbc.queryForString(connection, "select count(*) from " + createdSynonym)).isEqualTo("1");

        revoke("create public synonym", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create public synonym " + blockedSynonym + " for dual")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_333_002 revoking DROP PUBLIC SYNONYM blocks public synonym deletion")
    void tc333002RevokeDropPublicSynonym() throws Exception {
        String user = createManagedUser("QA_PUB_SYN_DROP");
        String droppableSynonym = DbTestSupport.uniqueName("QA_PUB_SYN_DROP_OK");
        String blockedSynonym = DbTestSupport.uniqueName("QA_PUB_SYN_DROP_BLOCK");
        registerCleanup(() -> DbTestSupport.dropPublicSynonymQuietly(jdbc, connection, blockedSynonym));

        jdbc.executeUpdate(connection, "create public synonym " + droppableSynonym + " for dual");
        jdbc.executeUpdate(connection, "create public synonym " + blockedSynonym + " for dual");

        grant(user, "drop public synonym");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop public synonym " + droppableSynonym));
        assertThatThrownBy(() -> jdbc.queryForString(connection, "select count(*) from " + droppableSynonym))
                .isInstanceOf(IllegalStateException.class);

        revoke("drop public synonym", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop public synonym " + blockedSynonym)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_333_003 revoking CREATE ANY SYNONYM blocks private synonym creation")
    void tc333003RevokeCreateAnySynonym() throws Exception {
        String owner = createManagedUser("QA_PRI_SYN_OWNER");
        String user = createManagedUser("QA_PRI_SYN");
        String createdSynonym = DbTestSupport.uniqueName("QA_PRI_SYN_OK");
        String blockedSynonym = DbTestSupport.uniqueName("QA_PRI_SYN_BLOCK");

        grant(user, "create any synonym");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create synonym " + owner + "." + createdSynonym + " for dual"));
        withUserConnection(owner, conn -> assertThat(jdbc.queryForString(conn, "select count(*) from " + createdSynonym)).isEqualTo("1"));

        revoke("create any synonym", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create synonym " + owner + "." + blockedSynonym + " for dual")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_333_004 revoking DROP ANY SYNONYM blocks private synonym deletion")
    void tc333004RevokeDropAnySynonym() throws Exception {
        String owner = createManagedUser("QA_PRI_SYN_OWNER");
        String user = createManagedUser("QA_PRI_SYN_DROP");
        String droppableSynonym = DbTestSupport.uniqueName("QA_PRI_SYN_DROP_OK");
        String blockedSynonym = DbTestSupport.uniqueName("QA_PRI_SYN_DROP_BLOCK");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + droppableSynonym + " for dual");
            jdbc.executeUpdate(conn, "create synonym " + blockedSynonym + " for dual");
        });

        grant(user, "drop any synonym");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop synonym " + owner + "." + droppableSynonym));
        assertThatThrownBy(() -> withUserConnection(owner, conn -> jdbc.queryForString(conn, "select count(*) from " + droppableSynonym)))
                .isInstanceOf(IllegalStateException.class);

        revoke("drop any synonym", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop synonym " + owner + "." + blockedSynonym)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_334_001 revoking CREATE ANY DIRECTORY blocks directory creation")
    void tc334001RevokeCreateAnyDirectory() throws Exception {
        String user = createManagedUser("QA_DIR_USER");
        String createdDirectory = DbTestSupport.uniqueName("QA_DIR_OK");
        String blockedDirectory = DbTestSupport.uniqueName("QA_DIR_BLOCK");

        registerCleanup(() -> dropDirectoryQuietly(createdDirectory));
        registerCleanup(() -> dropDirectoryQuietly(blockedDirectory));

        grant(user, "create any directory");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn,
                "create directory " + createdDirectory + " as '/tmp/altibase-qa-auto/" + createdDirectory.toLowerCase() + "'"));
        assertThat(directoryExists(createdDirectory)).isTrue();

        revoke("create any directory", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn,
                "create directory " + blockedDirectory + " as '/tmp/altibase-qa-auto/" + blockedDirectory.toLowerCase() + "'")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_334_002 revoking DROP ANY DIRECTORY blocks directory deletion")
    void tc334002RevokeDropAnyDirectory() throws Exception {
        String user = createManagedUser("QA_DIR_DROP");
        String droppableDirectory = DbTestSupport.uniqueName("QA_DROP_DIR_OK");
        String blockedDirectory = DbTestSupport.uniqueName("QA_DROP_DIR_BLOCK");

        registerCleanup(() -> dropDirectoryQuietly(blockedDirectory));
        jdbc.executeUpdate(connection, "create directory " + droppableDirectory + " as '/tmp/altibase-qa-auto/" + droppableDirectory.toLowerCase() + "'");
        jdbc.executeUpdate(connection, "create directory " + blockedDirectory + " as '/tmp/altibase-qa-auto/" + blockedDirectory.toLowerCase() + "'");

        grant(user, "drop any directory");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop directory " + droppableDirectory));
        assertThat(directoryExists(droppableDirectory)).isFalse();

        revoke("drop any directory", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop directory " + blockedDirectory)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_335_001 revoking CREATE MATERIALIZED VIEW blocks creation in own schema")
    void tc335001RevokeCreateMaterializedView() throws Exception {
        String user = createManagedUser("QA_MV_USER");
        String sourceTable = DbTestSupport.uniqueName("QA_MV_SRC");
        String createdView = DbTestSupport.uniqueName("QA_MV_OK");
        String blockedView = DbTestSupport.uniqueName("QA_MV_BLOCK");

        registerCleanup(() -> dropMaterializedViewQuietly(user + "." + createdView));
        registerCleanup(() -> dropMaterializedViewQuietly(user + "." + blockedView));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, user + "." + sourceTable));

        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "create table " + sourceTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + sourceTable + " values(1)");
            jdbc.executeUpdate(conn, "create materialized view " + createdView + " as select * from " + sourceTable);
            assertThat(jdbc.queryForString(conn, "select count(*) from " + createdView)).isEqualTo("1");
        });

        revoke("create materialized view", user);
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create materialized view " + blockedView + " as select * from " + sourceTable)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_335_002 revoking ALTER ANY MATERIALIZED VIEW blocks refresh option changes")
    void tc335002RevokeAlterAnyMaterializedView() throws Exception {
        String owner = createManagedUser("QA_MV_OWNER");
        String user = createManagedUser("QA_MV_ALTER");
        String sourceTable = DbTestSupport.uniqueName("QA_MV_ALT_SRC");
        String viewName = DbTestSupport.uniqueName("QA_MV_ALT");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + sourceTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + sourceTable + " values(1)");
            jdbc.executeUpdate(conn, "create materialized view " + viewName + " as select * from " + sourceTable);
        });

        grant(user, "alter any materialized view");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn,
                "alter materialized view " + owner + "." + viewName + " refresh complete on demand"));

        revoke("alter any materialized view", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn,
                "alter materialized view " + owner + "." + viewName + " refresh force on demand")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_335_003 revoking DROP ANY MATERIALIZED VIEW blocks removal")
    void tc335003RevokeDropAnyMaterializedView() throws Exception {
        String owner = createManagedUser("QA_DROP_MV_OWNER");
        String user = createManagedUser("QA_DROP_MV_USER");
        String sourceTable = "SRC";
        String droppableView = "MV1";
        String blockedView = "MV2";

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + sourceTable + "(c1 integer)");
            jdbc.executeUpdate(conn, "insert into " + sourceTable + " values(1)");
            jdbc.executeUpdate(conn, "create materialized view " + droppableView + " as select * from " + sourceTable);
            jdbc.executeUpdate(conn, "create materialized view " + blockedView + " as select * from " + sourceTable);
        });

        grant(user, "drop any materialized view");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop materialized view " + owner + "." + droppableView));
        assertThatThrownBy(() -> withUserConnection(owner, conn -> jdbc.queryForString(conn, "select count(*) from " + droppableView)))
                .isInstanceOf(IllegalStateException.class);

        revoke("drop any materialized view", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop materialized view " + owner + "." + blockedView)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_336_001 revoking CREATE ROLE blocks role creation")
    void tc336001RevokeCreateRole() throws Exception {
        String user = createManagedUser("QA_ROLE_USER");
        String createdRole = DbTestSupport.uniqueName("QA_ROLE_OK");
        String blockedRole = DbTestSupport.uniqueName("QA_ROLE_BLOCK");

        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, createdRole));
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, blockedRole));

        grant(user, "create role");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create role " + createdRole));
        assertThat(DbTestSupport.roleExists(jdbc, connection, createdRole)).isTrue();

        revoke("create role", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "create role " + blockedRole)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_336_002 revoking GRANT ANY ROLE blocks role delegation")
    void tc336002RevokeGrantAnyRole() throws Exception {
        String user = createManagedUser("QA_ROLE_GRANTOR");
        String grantee = createManagedUser("QA_ROLE_GRANTEE");
        String role = DbTestSupport.uniqueName("QA_ROLE_FOR_GRANT");
        String createdUser = DbTestSupport.uniqueName("QA_GRANTED_ROLE_USER");
        String blockedUser = DbTestSupport.uniqueName("QA_BLOCKED_ROLE_USER");

        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, role));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, createdUser));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, blockedUser));

        jdbc.executeUpdate(connection, "create role " + role);
        jdbc.executeUpdate(connection, "grant create user to " + role);

        grant(user, "grant any role");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "grant " + role + " to " + grantee));
        withUserConnection(grantee, conn -> jdbc.executeUpdate(conn, "create user " + createdUser + " identified by " + createdUser));
        assertThat(DbTestSupport.userExists(jdbc, connection, createdUser)).isTrue();

        revoke("grant any role", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "grant " + role + " to " + blockedUser)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_336_003 revoking DROP ANY ROLE blocks role deletion")
    void tc336003RevokeDropAnyRole() throws Exception {
        String user = createManagedUser("QA_DROP_ROLE_USER");
        String droppableRole = DbTestSupport.uniqueName("QA_DROP_ROLE_OK");
        String blockedRole = DbTestSupport.uniqueName("QA_DROP_ROLE_BLOCK");

        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, blockedRole));
        jdbc.executeUpdate(connection, "create role " + droppableRole);
        jdbc.executeUpdate(connection, "create role " + blockedRole);

        grant(user, "drop any role");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop role " + droppableRole));
        assertThat(DbTestSupport.roleExists(jdbc, connection, droppableRole)).isFalse();

        revoke("drop any role", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop role " + blockedRole)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_337_001 revoking CREATE ANY JOB blocks job creation")
    void tc337001RevokeCreateAnyJob() throws Exception {
        String user = createManagedUser("QA_JOB_USER");
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String createdJob = DbTestSupport.uniqueName("QA_JOB_OK");
        String blockedJob = DbTestSupport.uniqueName("QA_JOB_BLOCK");

        registerCleanup(() -> dropJobQuietly(createdJob));
        registerCleanup(() -> dropJobQuietly(blockedJob));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");

        grant(user, "create any job");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn,
                "create job " + createdJob + " exec sys." + procedureName + " start sysdate interval 1 month"));
        assertThat(jobExists(createdJob)).isTrue();

        revoke("create any job", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn,
                "create job " + blockedJob + " exec sys." + procedureName + " start sysdate interval 1 month")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_337_002 revoking ALTER ANY JOB blocks job changes")
    void tc337002RevokeAlterAnyJob() throws Exception {
        String user = createManagedUser("QA_ALT_JOB_USER");
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        grant(user, "alter any job");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter job " + jobName + " set enable"));
        assertThat(jobEnabled(jobName)).isTrue();

        revoke("alter any job", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "alter job " + jobName + " set disable")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_337_003 revoking DROP ANY JOB blocks job deletion")
    void tc337003RevokeDropAnyJob() throws Exception {
        String user = createManagedUser("QA_DROP_JOB_USER");
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String droppableJob = DbTestSupport.uniqueName("QA_JOB_OK");
        String blockedJob = DbTestSupport.uniqueName("QA_JOB_BLOCK");

        registerCleanup(() -> dropJobQuietly(blockedJob));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");
        jdbc.executeUpdate(connection, "create job " + droppableJob + " exec " + procedureName + " start sysdate interval 1 month");
        jdbc.executeUpdate(connection, "create job " + blockedJob + " exec " + procedureName + " start sysdate interval 1 month");

        grant(user, "drop any job");
        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop job " + droppableJob));
        assertThat(jobExists(droppableJob)).isFalse();

        revoke("drop any job", user);
        assertThatThrownBy(() -> withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop job " + blockedJob)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_338_001 revoking REFERENCES blocks cross-schema foreign-key creation")
    void tc338001RevokeReferencesOnObject() throws Exception {
        String owner = createManagedUser("QA_REF_OWNER");
        String user = createManagedUser("QA_REF_USER");
        String parentTable = "BOOK";
        String childTable = "REF_BOOK";

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + parentTable + "(id integer primary key)"));
        jdbc.executeUpdate(connection, "grant references on " + owner + "." + parentTable + " to " + user);

        withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create table " + childTable + "(id integer primary key, book_id integer references " + owner + "." + parentTable + "(id))"));
        assertThat(hasImportedKeys(user, childTable)).isTrue();

        withUserConnection(user, conn -> jdbc.executeUpdate(conn, "drop table " + childTable + " cascade"));
        jdbc.executeUpdate(connection, "revoke references on " + owner + "." + parentTable + " from " + user);
        assertThatThrownBy(() -> withUserConnection(user, conn ->
                jdbc.executeUpdate(conn, "create table " + childTable + "_2(id integer primary key, book_id integer references " + owner + "." + parentTable + "(id))")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_339_001 revoking REFERENCES CASCADE CONSTRAINTS removes dependent foreign keys")
    void tc339001RevokeReferencesCascadeConstraints() throws Exception {
        String owner = createManagedUser("QA_CASCADE_OWNER");
        String user = createManagedUser("QA_CASCADE_USER");
        String parentTable = "BOOK";
        String childTable = "BOOK_CHILD";

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + parentTable + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + parentTable + " values(1)");
        });
        jdbc.executeUpdate(connection, "grant references on " + owner + "." + parentTable + " to " + user);

        withUserConnection(user, conn -> {
            jdbc.executeUpdate(conn, "create table " + childTable + "(id integer primary key, book_id integer, constraint " +
                    DbTestSupport.uniqueName("QA_FK_REF") + " foreign key(book_id) references " + owner + "." + parentTable + "(id))");
            jdbc.executeUpdate(conn, "insert into " + childTable + " values(1, 1)");
        });
        assertThat(hasImportedKeys(user, childTable)).isTrue();

        jdbc.executeUpdate(connection, "revoke references on " + owner + "." + parentTable + " from " + user + " cascade constraints");

        assertThat(hasImportedKeys(user, childTable)).isFalse();
        jdbc.executeUpdate(connection, "delete from " + owner + "." + parentTable + " where id = 1");
        assertThat(jdbc.query(connection, "select * from " + owner + "." + parentTable).size()).isZero();
    }

    private String createManagedUser(String prefix) {
        String user = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);
        return user;
    }

    private void grant(String grantee, String privilege) {
        try {
            jdbc.executeUpdate(connection, "grant " + privilege + " to " + grantee);
        } catch (IllegalStateException e) {
            if (isAlreadyGranted(e)) {
                return;
            }
            throw e;
        }
    }

    private void revoke(String privilege, String grantee) {
        jdbc.executeUpdate(connection, "revoke " + privilege + " from " + grantee);
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

    private boolean jobExists(String jobName) {
        return jdbc.exists(connection, "select job_name from system_.sys_jobs_ where job_name = '" + jobName + "'");
    }

    private boolean jobEnabled(String jobName) {
        return "T".equalsIgnoreCase(jdbc.queryForString(connection,
                "select is_enable from system_.sys_jobs_ where job_name = '" + jobName + "'"));
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
