package com.altibase.qa.security;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserRoleJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_292_001 CREATE USER grants the default access needed for basic work")
    void tc292001CreateUserAccess() throws Exception {
        String userName = DbTestSupport.uniqueName("QA_ACCESS_USER");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(
                connection,
                "create user " + userName + " identified by " + userName + " default tablespace SYS_TBS_MEM_DATA"
        );

        withUserConnection(userName, userName, userConn -> {
            jdbc.executeUpdate(userConn, "create table ACCESS_OK_TB(c1 int)");
            assertThat(jdbc.queryForString(userConn, "select count(*) from ACCESS_OK_TB")).isEqualTo("0");
            assertThatThrownBy(() ->
                    jdbc.executeUpdate(userConn, "create table ACCESS_FAIL_TB(c1 int) tablespace SYS_TBS_DISK_DATA"))
                    .isInstanceOf(IllegalStateException.class);
        });
    }

    @Test
    @DisplayName("TC_292_002 CREATE USER can set the default tablespace")
    void tc292002CreateUserWithDefaultTablespace() throws Exception {
        String userName = DbTestSupport.uniqueName("QA_DEF_TBS_USER");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(
                connection,
                "create user " + userName + " identified by " + userName + " default tablespace SYS_TBS_DISK_DATA"
        );

        assertThat(userIntValue(userName, "DEFAULT_TBS_ID")).isEqualTo(2);
        withUserConnection(userName, userName, userConn ->
                jdbc.executeUpdate(userConn, "create table DEFAULT_TBS_TB(c1 int)"));
    }

    @Test
    @DisplayName("TC_293_001 TCP access can be disabled and re-enabled for a user")
    void tc293001ToggleTcpAccess() {
        String userName = DbTestSupport.uniqueName("QA_TCP_USER");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName + " disable tcp");
        assertThat(userStringValue(userName, "DISABLE_TCP")).isEqualTo("T");

        assertThatThrownBy(() -> jdbc.open(userName, userName))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "alter user " + userName + " enable tcp");
        assertThat(userStringValue(userName, "DISABLE_TCP")).isEqualTo("F");

        Connection userConn = jdbc.open(userName, userName);
        try {
            assertThat(jdbc.queryForString(userConn, "select count(*) from dual")).isEqualTo("1");
        } finally {
            jdbc.closeQuietly(userConn);
        }
    }

    @Test
    @DisplayName("TC_294_001 CREATE USER can set PASSWORD_LOCK_TIME")
    void tc294001CreateUserWithPasswordLockTime() {
        String userName = DbTestSupport.uniqueName("QA_LOCK_TIME");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(
                connection,
                "create user " + userName + " identified by " + userName + " limit (password_lock_time 3)"
        );

        assertThat(userIntValue(userName, "PASSWORD_LOCK_TIME")).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_295_001 CREATE USER can set FAILED_LOGIN_ATTEMPTS and lock the account")
    void tc295001FailedLoginAttemptsLocksAccount() {
        String userName = DbTestSupport.uniqueName("QA_LOGIN_LOCK");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(
                connection,
                "create user " + userName + " identified by " + userName +
                        " limit (failed_login_attempts 2, password_lock_time 1)"
        );

        assertThat(userIntValue(userName, "FAILED_LOGIN_ATTEMPTS")).isEqualTo(2);

        assertThatThrownBy(() -> jdbc.open(userName, "WRONG_PASSWORD"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> jdbc.open(userName, "WRONG_PASSWORD"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> jdbc.open(userName, userName))
                .isInstanceOf(IllegalStateException.class);

        assertThat(userStringValue(userName, "ACCOUNT_LOCK")).isEqualTo("L");
        assertThat(userIntValue(userName, "FAILED_LOGIN_COUNT")).isEqualTo(2);
    }

    @Test
    @DisplayName("TC_296_001 CREATE USER can set PASSWORD_LIFE_TIME")
    void tc296001CreateUserWithPasswordLifeTime() {
        String userName = DbTestSupport.uniqueName("QA_PW_LIFE");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(
                connection,
                "create user " + userName + " identified by " + userName + " limit (password_life_time 4)"
        );

        assertThat(userIntValue(userName, "PASSWORD_LIFE_TIME")).isEqualTo(4);
    }

    @Test
    @DisplayName("TC_297_001 CREATE USER can set PASSWORD_REUSE_TIME")
    void tc297001CreateUserWithPasswordReuseTime() {
        String userName = DbTestSupport.uniqueName("QA_PW_REUSE");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(
                connection,
                "create user " + userName + " identified by " + userName + " limit (password_reuse_time 5)"
        );

        assertThat(userIntValue(userName, "PASSWORD_REUSE_TIME")).isEqualTo(5);
    }

    @Test
    @DisplayName("TC_298_001 ALTER USER IDENTIFIED BY changes the password")
    void tc298001AlterUserPassword() {
        String userName = DbTestSupport.uniqueName("QA_PW_CHANGE");
        String newPassword = "CHANGEDPW1";
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);
        jdbc.executeUpdate(connection, "alter user " + userName + " identified by " + newPassword);

        assertThatThrownBy(() -> jdbc.open(userName, userName))
                .isInstanceOf(IllegalStateException.class);

        Connection userConn = jdbc.open(userName, newPassword);
        try {
            assertThat(jdbc.queryForString(userConn, "select count(*) from dual")).isEqualTo("1");
        } finally {
            jdbc.closeQuietly(userConn);
        }
    }

    @Test
    @DisplayName("TC_299_001 ALTER USER can change the default tablespace")
    void tc299001AlterDefaultTablespace() {
        String userName = DbTestSupport.uniqueName("QA_ALT_DEF_TBS");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(
                connection,
                "create user " + userName + " identified by " + userName + " default tablespace SYS_TBS_MEM_DATA"
        );
        assertThat(userIntValue(userName, "DEFAULT_TBS_ID")).isEqualTo(1);

        jdbc.executeUpdate(connection, "alter user " + userName + " default tablespace SYS_TBS_DISK_DATA");
        assertThat(userIntValue(userName, "DEFAULT_TBS_ID")).isEqualTo(2);
    }

    @Test
    @DisplayName("TC_302_001 ALTER USER can change the temporary tablespace")
    void tc302001AlterTemporaryTablespace() {
        String tablespaceName = DbTestSupport.uniqueName("QA_TEMP_TBS");
        String userName = DbTestSupport.uniqueName("QA_TEMP_USER");
        String tempFile = "/tmp/" + tablespaceName.toLowerCase() + ".tmp";
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));
        registerCleanup(() -> dropTemporaryTablespaceQuietly(tablespaceName));

        jdbc.executeUpdate(
                connection,
                "create temporary tablespace " + tablespaceName + " tempfile '" + tempFile + "' size 5M autoextend off"
        );
        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);

        int originalTempTablespace = userIntValue(userName, "TEMP_TBS_ID");
        jdbc.executeUpdate(connection, "alter user " + userName + " temporary tablespace " + tablespaceName);

        assertThat(userIntValue(userName, "TEMP_TBS_ID")).isNotEqualTo(originalTempTablespace);
    }

    @Test
    @DisplayName("TC_303_001 create and drop a user")
    void tc303001CreateAndDropUser() {
        String userName = DbTestSupport.uniqueName("QA_USER");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);
        assertThat(DbTestSupport.userExists(jdbc, connection, userName)).isTrue();

        jdbc.executeUpdate(connection, "drop user " + userName);
        assertThat(DbTestSupport.userExists(jdbc, connection, userName)).isFalse();
    }

    @Test
    @DisplayName("TC_304_001 DROP USER CASCADE removes owned objects")
    void tc304001DropUserCascade() {
        String userName = DbTestSupport.uniqueName("QA_UC");
        String tableName = "OWNED_TB";
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);

        Connection userConnection = jdbc.open(userName, userName);
        try {
            jdbc.executeUpdate(userConnection, "create table " + tableName + "(c1 int)");
        } finally {
            jdbc.closeQuietly(userConnection);
        }

        jdbc.executeUpdate(connection, "drop user " + userName + " cascade");
        assertThat(DbTestSupport.userExists(jdbc, connection, userName)).isFalse();
    }

    @Test
    @DisplayName("TC_308_001 create a role")
    void tc308001CreateRole() {
        String roleName = DbTestSupport.uniqueName("QA_ROLE");
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, roleName));

        jdbc.executeUpdate(connection, "create role " + roleName);

        assertThat(DbTestSupport.roleExists(jdbc, connection, roleName)).isTrue();
    }

    @Test
    @DisplayName("TC_309_001 drop a role")
    void tc309001DropRole() {
        String roleName = DbTestSupport.uniqueName("QA_ROLE_DROP");

        jdbc.executeUpdate(connection, "create role " + roleName);
        jdbc.executeUpdate(connection, "drop role " + roleName);

        assertThat(DbTestSupport.roleExists(jdbc, connection, roleName)).isFalse();
    }

    @Test
    @DisplayName("TC_310_001 grant CREATE USER through a role")
    void tc310001GrantCreateUserPrivilegeViaRole() {
        String roleName = DbTestSupport.uniqueName("QA_ROLE_USER");
        String grantee = DbTestSupport.uniqueName("QA_GRANTEE");
        String created = DbTestSupport.uniqueName("QA_CREATED");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, created));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, grantee));
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, roleName));

        jdbc.executeUpdate(connection, "create role " + roleName);
        jdbc.executeUpdate(connection, "create user " + grantee + " identified by " + grantee);
        jdbc.executeUpdate(connection, "grant create user, drop user to " + roleName);
        jdbc.executeUpdate(connection, "grant " + roleName + " to " + grantee);

        Connection granteeConn = jdbc.open(grantee, grantee);
        try {
            jdbc.executeUpdate(granteeConn, "create user " + created + " identified by " + created);
        } finally {
            jdbc.closeQuietly(granteeConn);
        }

        assertThat(DbTestSupport.userExists(jdbc, connection, created)).isTrue();
    }

    @Test
    @DisplayName("TC_311_004 SELECT ANY TABLE allows reading another schema's table")
    void tc311004GrantSelectAnyTable() {
        String owner = DbTestSupport.uniqueName("QA_OWNER");
        String user = DbTestSupport.uniqueName("QA_SEL");
        String role = DbTestSupport.uniqueName("QA_SEL_ROLE");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, owner));
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, role));

        jdbc.executeUpdate(connection, "create user " + owner + " identified by " + owner);
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);
        jdbc.executeUpdate(connection, "create role " + role);
        jdbc.executeUpdate(connection, "grant select any table to " + role);
        jdbc.executeUpdate(connection, "grant " + role + " to " + user);

        Connection ownerConn = jdbc.open(owner, owner);
        try {
            jdbc.executeUpdate(ownerConn, "create table OWNED_SELECT_TB(c1 int)");
            jdbc.executeUpdate(ownerConn, "insert into OWNED_SELECT_TB values(10)");
        } finally {
            jdbc.closeQuietly(ownerConn);
        }

        Connection userConn = jdbc.open(user, user);
        try {
            assertThat(jdbc.queryForString(userConn, "select c1 from " + owner + ".OWNED_SELECT_TB")).isEqualTo("10");
        } finally {
            jdbc.closeQuietly(userConn);
        }
    }

    @Test
    @DisplayName("TC_311_005 INSERT ANY TABLE allows inserting into another schema's table")
    void tc311005GrantInsertAnyTable() {
        String owner = DbTestSupport.uniqueName("QA_OWNER_INS");
        String user = DbTestSupport.uniqueName("QA_INS");
        String role = DbTestSupport.uniqueName("QA_INS_ROLE");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, owner));
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, role));

        jdbc.executeUpdate(connection, "create user " + owner + " identified by " + owner);
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);
        jdbc.executeUpdate(connection, "create role " + role);
        jdbc.executeUpdate(connection, "grant insert any table to " + role);
        jdbc.executeUpdate(connection, "grant " + role + " to " + user);

        Connection ownerConn = jdbc.open(owner, owner);
        try {
            jdbc.executeUpdate(ownerConn, "create table OWNED_INSERT_TB(c1 int)");
        } finally {
            jdbc.closeQuietly(ownerConn);
        }

        Connection userConn = jdbc.open(user, user);
        try {
            jdbc.executeUpdate(userConn, "insert into " + owner + ".OWNED_INSERT_TB values(10)");
        } finally {
            jdbc.closeQuietly(userConn);
        }

        Connection ownerConn2 = jdbc.open(owner, owner);
        try {
            assertThat(jdbc.queryForString(ownerConn2, "select c1 from OWNED_INSERT_TB")).isEqualTo("10");
        } finally {
            jdbc.closeQuietly(ownerConn2);
        }
    }

    @Test
    @DisplayName("TC_311_006 DELETE ANY TABLE allows deleting from another schema's table")
    void tc311006GrantDeleteAnyTable() {
        String owner = DbTestSupport.uniqueName("QA_OWNER_DEL");
        String user = DbTestSupport.uniqueName("QA_DEL");
        String role = DbTestSupport.uniqueName("QA_DEL_ROLE");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, owner));
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, role));

        jdbc.executeUpdate(connection, "create user " + owner + " identified by " + owner);
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);
        jdbc.executeUpdate(connection, "create role " + role);
        jdbc.executeUpdate(connection, "grant delete any table to " + role);
        jdbc.executeUpdate(connection, "grant " + role + " to " + user);

        Connection ownerConn = jdbc.open(owner, owner);
        try {
            jdbc.executeUpdate(ownerConn, "create table OWNED_DELETE_TB(c1 int)");
            jdbc.executeUpdate(ownerConn, "insert into OWNED_DELETE_TB values(10)");
        } finally {
            jdbc.closeQuietly(ownerConn);
        }

        Connection userConn = jdbc.open(user, user);
        try {
            jdbc.executeUpdate(userConn, "delete from " + owner + ".OWNED_DELETE_TB");
        } finally {
            jdbc.closeQuietly(userConn);
        }

        Connection ownerConn2 = jdbc.open(owner, owner);
        try {
            assertThat(jdbc.query(ownerConn2, "select * from OWNED_DELETE_TB").size()).isZero();
        } finally {
            jdbc.closeQuietly(ownerConn2);
        }
    }

    @Test
    @DisplayName("TC_311_007 UPDATE ANY TABLE allows updating another schema's table")
    void tc311007GrantUpdateAnyTable() {
        String owner = DbTestSupport.uniqueName("QA_OWNER_UPD");
        String user = DbTestSupport.uniqueName("QA_UPD");
        String role = DbTestSupport.uniqueName("QA_UPD_ROLE");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, owner));
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, role));

        jdbc.executeUpdate(connection, "create user " + owner + " identified by " + owner);
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);
        jdbc.executeUpdate(connection, "create role " + role);
        jdbc.executeUpdate(connection, "grant update any table to " + role);
        jdbc.executeUpdate(connection, "grant " + role + " to " + user);

        Connection ownerConn = jdbc.open(owner, owner);
        try {
            jdbc.executeUpdate(ownerConn, "create table OWNED_UPDATE_TB(c1 int)");
            jdbc.executeUpdate(ownerConn, "insert into OWNED_UPDATE_TB values(10)");
        } finally {
            jdbc.closeQuietly(ownerConn);
        }

        Connection userConn = jdbc.open(user, user);
        try {
            jdbc.executeUpdate(userConn, "update " + owner + ".OWNED_UPDATE_TB set c1 = 20");
        } finally {
            jdbc.closeQuietly(userConn);
        }

        Connection ownerConn2 = jdbc.open(owner, owner);
        try {
            assertThat(jdbc.queryForString(ownerConn2, "select c1 from OWNED_UPDATE_TB")).isEqualTo("20");
        } finally {
            jdbc.closeQuietly(ownerConn2);
        }
    }

    @Test
    @DisplayName("Additional negative case: inserting into another schema's table fails without privilege")
    void tc311Neg001InsertWithoutPrivilegeFails() {
        String owner = DbTestSupport.uniqueName("QA_OWNER_NOPRIV");
        String user = DbTestSupport.uniqueName("QA_NOPRIV");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, owner));

        jdbc.executeUpdate(connection, "create user " + owner + " identified by " + owner);
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);

        Connection ownerConn = jdbc.open(owner, owner);
        try {
            jdbc.executeUpdate(ownerConn, "create table OWNED_NO_PRIV_TB(c1 int)");
        } finally {
            jdbc.closeQuietly(ownerConn);
        }

        Connection userConn = jdbc.open(user, user);
        try {
            assertThatThrownBy(() -> jdbc.executeUpdate(userConn, "insert into " + owner + ".OWNED_NO_PRIV_TB values(10)"))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            jdbc.closeQuietly(userConn);
        }
    }

    @Test
    @DisplayName("Additional negative case: login fails with an invalid password")
    void tc704001LoginFailure() {
        String userName = DbTestSupport.uniqueName("QA_LOGIN");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, userName));

        jdbc.executeUpdate(connection, "create user " + userName + " identified by " + userName);

        assertThatThrownBy(() -> jdbc.open(userName, "WRONG_PASSWORD"))
                .isInstanceOf(IllegalStateException.class);
    }

    private String userStringValue(String userName, String columnName) {
        return jdbc.queryForString(
                connection,
                "select " + columnName + " from system_.sys_users_ where user_name = '" + userName + "'"
        );
    }

    private int userIntValue(String userName, String columnName) {
        return Integer.parseInt(userStringValue(userName, columnName));
    }

    private void dropTemporaryTablespaceQuietly(String tablespaceName) {
        try {
            jdbc.executeUpdate(connection, "drop tablespace " + tablespaceName + " including contents and datafiles");
        } catch (Exception ignored) {
        }
    }

    private void withUserConnection(String userName, String password, SqlConnectionConsumer consumer) throws Exception {
        Connection userConn = jdbc.open(userName, password);
        try {
            consumer.accept(userConn);
        } finally {
            jdbc.closeQuietly(userConn);
        }
    }

    @FunctionalInterface
    private interface SqlConnectionConsumer {
        void accept(Connection connection) throws Exception;
    }
}
