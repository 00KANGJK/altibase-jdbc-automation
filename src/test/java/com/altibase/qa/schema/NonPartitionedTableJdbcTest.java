package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NonPartitionedTableJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_048_001 create a table in an explicitly specified owner schema")
    void tc048001CreateTableWithOwner() {
        String owner = DbTestSupport.uniqueName("QA_TB_OWNER");
        String tableName = DbTestSupport.uniqueName("QA_OWNER_TB");
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, owner));

        jdbc.executeUpdate(connection, "create user " + owner + " identified by " + owner);
        jdbc.executeUpdate(connection, "create table " + owner + "." + tableName + "(c1 int, c2 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + owner + "." + tableName + " values (1, 'A')");

        assertThat(DbTestSupport.tableExists(connection, owner, tableName)).isTrue();
        assertThat(jdbc.queryForString(connection,
                "select c2 from " + owner + "." + tableName + " where c1 = 1")).isEqualTo("A");
    }

    @Test
    @DisplayName("TC_050_001 MAXROWS를 초과하는 행 삽입을 막는다")
    void tc050001MaxRowsLimit() {
        String tableName = DbTestSupport.uniqueName("QA_TB_MAXROWS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 int) maxrows 2");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (1, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (2, 20)");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values (3, 30)"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_049_001 기본 키 제약조건이 생성되고 NULL 입력을 막는다")
    void tc049001PrimaryKeyConstraint() {
        String tableName = DbTestSupport.uniqueName("QA_TB_PK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20) primary key)");

        assertThat(DbTestSupport.tableExists(connection, null, tableName)).isTrue();
        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + "(c1) values (1)"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_049_002 유니크 제약조건이 중복 입력을 막는다")
    void tc049002UniqueConstraint() {
        String tableName = DbTestSupport.uniqueName("QA_TB_UK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20) unique)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (1, 'A')");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values (2, 'A')"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_049_003 체크 제약조건이 허용 범위 밖 입력을 막는다")
    void tc049003CheckConstraint() {
        String tableName = DbTestSupport.uniqueName("QA_TB_CHK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(1) check(c2 in ('A','C')))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (1, 'A')");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values (2, 'B')"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_051_001 ON DELETE NO ACTION은 부모 삭제를 막는다")
    void tc051001OnDeleteNoAction() {
        String parent = DbTestSupport.uniqueName("QA_PARENT_NOACT");
        String child = DbTestSupport.uniqueName("QA_CHILD_NOACT");
        String constraintName = DbTestSupport.uniqueName("QA_FK_NOACT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(fore varchar(10) primary key)");
        jdbc.executeUpdate(connection, "create table " + child + "(c1 int, fore varchar(20), constraint " + constraintName + " foreign key(fore) references " + parent + "(fore) on delete no action)");
        jdbc.executeUpdate(connection, "insert into " + parent + " values ('P1')");
        jdbc.executeUpdate(connection, "insert into " + child + " values (1, 'P1')");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "delete from " + parent + " where fore = 'P1'"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_052_001 ON DELETE CASCADE는 자식 행을 함께 삭제한다")
    void tc052001OnDeleteCascade() {
        String parent = DbTestSupport.uniqueName("QA_PARENT_CAS");
        String child = DbTestSupport.uniqueName("QA_CHILD_CAS");
        String constraintName = DbTestSupport.uniqueName("QA_FK_CAS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(fore varchar(10) primary key)");
        jdbc.executeUpdate(connection, "create table " + child + "(c1 int, fore varchar(20), constraint " + constraintName + " foreign key(fore) references " + parent + "(fore) on delete cascade)");
        jdbc.executeUpdate(connection, "insert into " + parent + " values ('P1')");
        jdbc.executeUpdate(connection, "insert into " + child + " values (1, 'P1')");
        jdbc.executeUpdate(connection, "delete from " + parent + " where fore = 'P1'");

        assertThat(jdbc.query(connection, "select * from " + child).size()).isZero();
    }

    @Test
    @DisplayName("TC_053_001 ON DELETE SET NULL은 자식 참조값을 NULL로 바꾼다")
    void tc053001OnDeleteSetNull() {
        String parent = DbTestSupport.uniqueName("QA_PARENT_NULL");
        String child = DbTestSupport.uniqueName("QA_CHILD_NULL");
        String constraintName = DbTestSupport.uniqueName("QA_FK_SETNULL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(fore varchar(10) primary key)");
        jdbc.executeUpdate(connection, "create table " + child + "(c1 int, fore varchar(20), constraint " + constraintName + " foreign key(fore) references " + parent + "(fore) on delete set null)");
        jdbc.executeUpdate(connection, "insert into " + parent + " values ('P1')");
        jdbc.executeUpdate(connection, "insert into " + child + " values (1, 'P1')");
        jdbc.executeUpdate(connection, "delete from " + parent + " where fore = 'P1'");

        assertThat(jdbc.queryForString(connection, "select fore from " + child + " where c1 = 1")).isNull();
    }

    @Test
    @DisplayName("TC_055_001 테이블에 칼럼을 추가할 수 있다")
    void tc055001AddColumn() {
        String tableName = DbTestSupport.uniqueName("QA_TB_ADD");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " add column (c2 integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c2) values (1, 2)");

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1")).isEqualTo("2");
    }

    @Test
    @DisplayName("TC_055_002 테이블에 기본 키 칼럼을 추가할 수 있다")
    void tc055002AddPrimaryKeyColumn() {
        String tableName = DbTestSupport.uniqueName("QA_TB_ADD_PK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " add column (c3 char(10) primary key)");
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c3) values (1, 'A')");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c3) values (2, 'A')"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_055_003 테이블에 유니크 칼럼을 추가할 수 있다")
    void tc055003AddUniqueColumn() {
        String tableName = DbTestSupport.uniqueName("QA_TB_ADD_UK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " add column (c4 varchar(10) unique)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (1, 'A')");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values (2, 'A')"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_056_001 칼럼 기본값을 설정할 수 있다")
    void tc056001SetDefault() {
        String tableName = DbTestSupport.uniqueName("QA_TB_DEF");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " alter(c1 set default 119)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(default)");

        assertThat(jdbc.queryForString(connection, "select c1 from " + tableName)).isEqualTo("119");
    }

    @Test
    @DisplayName("TC_056_002 칼럼 기본값을 제거할 수 있다")
    void tc056002DropDefault() {
        String tableName = DbTestSupport.uniqueName("QA_TB_DROP_DEF");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int default 119)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " alter(c1 drop default)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(default)");

        assertThat(jdbc.queryForString(connection, "select c1 from " + tableName)).isNull();
    }

    @Test
    @DisplayName("TC_056_003 칼럼을 NOT NULL로 변경하면 NULL 입력을 막는다")
    void tc056003SetNotNull() {
        String tableName = DbTestSupport.uniqueName("QA_TB_NOTNULL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(v1 varchar(10))");
        jdbc.executeUpdate(connection, "alter table " + tableName + " alter(v1 not null)");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values(NULL)"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_056_004 칼럼을 NULL 허용으로 변경할 수 있다")
    void tc056004SetNull() {
        String tableName = DbTestSupport.uniqueName("QA_TB_NULL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(v1 varchar(10) not null)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " alter(v1 null)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(NULL)");

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_057_001 테이블의 칼럼을 제거할 수 있다")
    void tc057001DropColumns() {
        String tableName = DbTestSupport.uniqueName("QA_TB_DROP_COL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(v1 varchar(10), c2 int, c3 number)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop column (c2, c3)");
        jdbc.executeUpdate(connection, "insert into " + tableName + "(v1) values('A')");

        assertThat(jdbc.queryForString(connection, "select v1 from " + tableName)).isEqualTo("A");
    }

    @Test
    @DisplayName("TC_057_002 테이블의 유니크 칼럼을 제거할 수 있다")
    void tc057002DropUniqueColumn() {
        String tableName = DbTestSupport.uniqueName("QA_TB_DROP_UQCOL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int unique, c2 int)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop column (c1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c2) values (100)");

        assertThat(DbTestSupport.columnExists(connection, null, tableName, "C1")).isFalse();
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName)).isEqualTo("100");
    }

    @Test
    @DisplayName("TC_057_003 테이블의 기본 키 칼럼을 제거할 수 있다")
    void tc057003DropPrimaryKeyColumn() {
        String tableName = DbTestSupport.uniqueName("QA_TB_DROP_PKCOL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int primary key, c2 int)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop column (c1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c2) values (200)");

        assertThat(DbTestSupport.columnExists(connection, null, tableName, "C1")).isFalse();
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName)).isEqualTo("200");
    }

    @Test
    @DisplayName("TC_058_001 테이블을 삭제할 수 있다")
    void tc058001DropTable() {
        String tableName = DbTestSupport.uniqueName("QA_TB_DROP");

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "drop table " + tableName);

        assertThat(DbTestSupport.tableExists(connection, null, tableName)).isFalse();
    }

    @Test
    @DisplayName("TC_059_001 CASCADE로 참조 제약을 가진 테이블을 삭제할 수 있다")
    void tc059001DropTableCascade() {
        String parent = DbTestSupport.uniqueName("QA_PARENT_DROP");
        String child = DbTestSupport.uniqueName("QA_CHILD_DROP");
        String constraintName = DbTestSupport.uniqueName("QA_FK_DROP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(fore varchar(10) primary key)");
        jdbc.executeUpdate(connection, "create table " + child + "(c1 int, fore varchar(20), constraint " + constraintName + " foreign key(fore) references " + parent + "(fore) on delete no action)");
        jdbc.executeUpdate(connection, "drop table " + parent + " cascade");

        assertThat(DbTestSupport.tableExists(connection, null, parent)).isFalse();
    }

    @Test
    @DisplayName("TC_073_001 ON COMMIT DELETE ROWS 임시 테이블은 커밋 후 행이 지워진다")
    void tc073001TemporaryTableDeleteRows() {
        String tablespaceName = DbTestSupport.uniqueName("QA_VOL_TBS_DEL");
        String tableName = DbTestSupport.uniqueName("QA_TMP_DEL");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create volatile data tablespace " + tablespaceName + " size 4M");
        jdbc.executeUpdate(connection, "create temporary table " + tableName + "(c1 int) on commit delete rows tablespace " + tablespaceName);
        jdbc.begin(connection);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.commit(connection);

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isZero();
    }

    @Test
    @DisplayName("TC_074_001 ON COMMIT PRESERVE ROWS 임시 테이블은 커밋 후 행이 유지된다")
    void tc074001TemporaryTablePreserveRows() {
        String tablespaceName = DbTestSupport.uniqueName("QA_VOL_TBS_PRE");
        String tableName = DbTestSupport.uniqueName("QA_TMP_PRE");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create volatile data tablespace " + tablespaceName + " size 4M");
        jdbc.executeUpdate(connection, "create temporary table " + tableName + "(c1 int) on commit preserve rows tablespace " + tablespaceName);
        jdbc.begin(connection);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.commit(connection);

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_054_001 create a table in a specified tablespace")
    void tc054001CreateTableInSpecifiedTablespace() {
        String tablespaceName = DbTestSupport.uniqueName("QA_TB_TBS");
        String tableName = DbTestSupport.uniqueName("QA_TB_IN_TBS");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create memory tablespace " + tablespaceName + " size 8M");
        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int) tablespace " + tablespaceName);

        assertThat(jdbc.queryForString(connection,
                "select tbs_name from system_.sys_tables_ where table_name = '" + tableName + "'"))
                .isEqualTo(tablespaceName);
    }

    @Test
    @DisplayName("Additional negative case: creating a table in a missing owner schema fails")
    void createTableInMissingOwnerFails() {
        String tableName = DbTestSupport.uniqueName("QA_MISSING_OWNER_TB");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "create table MISSING_OWNER." + tableName + "(c1 int)"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: creating a table in a missing tablespace fails")
    void createTableInMissingTablespaceFails() {
        String tableName = DbTestSupport.uniqueName("QA_MISSING_TBS_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int) tablespace MISSING_TBS"))
                .isInstanceOf(IllegalStateException.class);
    }
}
