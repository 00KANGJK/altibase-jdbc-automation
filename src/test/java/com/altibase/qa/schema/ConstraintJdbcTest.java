package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConstraintJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_078_001 create table with named primary key constraint")
    void tc078001CreateTableWithConstraint() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_CONS_TB");
        String constraintName = DbTestSupport.uniqueName("QA_PK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection,
                "create table " + tableName + "(i1 timestamp constraint " + constraintName + " primary key, i2 integer, i3 date)");

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("PK_NAME")).isEqualTo(constraintName);
        }
    }

    @Test
    @DisplayName("TC_079_001 alter table add column with foreign key constraint")
    void tc079001AddForeignKeyConstraintViaAlter() {
        String parent = DbTestSupport.uniqueName("QA_BOOK");
        String child = DbTestSupport.uniqueName("QA_INV");
        String fkName = DbTestSupport.uniqueName("QA_FK_ALTER");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(isbn char(10) primary key)");
        jdbc.executeUpdate(connection, "create table " + child + "(id integer)");
        jdbc.executeUpdate(connection,
                "alter table " + child + " add column (isbn char(10) constraint " + fkName + " references " + parent + "(isbn))");

        jdbc.executeUpdate(connection, "insert into " + parent + " values('0000000001')");
        jdbc.executeUpdate(connection, "insert into " + child + "(id, isbn) values(1, '0000000001')");

        assertThat(jdbc.queryForString(connection, "select isbn from " + child + " where id = 1")).isEqualTo("0000000001");
    }

    @Test
    @DisplayName("TC_079_002 alter table add column with primary key constraint")
    void tc079002AddPrimaryKeyConstraintViaAlter() {
        String tableName = DbTestSupport.uniqueName("QA_BOOKS");
        String constraintName = DbTestSupport.uniqueName("QA_PK_ALTER");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(bno integer)");
        jdbc.executeUpdate(connection,
                "alter table " + tableName + " add column (isbn char(10) constraint " + constraintName + " primary key, edition integer default 1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + "(bno, isbn) values(1, '0000000001')");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "insert into " + tableName + "(bno, isbn) values(2, '0000000001')"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_080_001 alter table drop unique(column)")
    void tc080001DropUniqueConstraintByColumn() {
        String tableName = DbTestSupport.uniqueName("QA_DROP_UK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(bno integer unique)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop unique(bno)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isEqualTo(2);
    }

    @Test
    @DisplayName("TC_080_002 alter table drop named constraint")
    void tc080002DropNamedConstraint() {
        String tableName = DbTestSupport.uniqueName("QA_DROP_CONS");
        String constraintName = DbTestSupport.uniqueName("QA_UK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(bno integer constraint " + constraintName + " unique)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop constraint " + constraintName);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isEqualTo(2);
    }

    @Test
    @DisplayName("TC_081_001 modify constraint by adding foreign key")
    void tc081001ModifyConstraintAddForeignKey() {
        String parent = DbTestSupport.uniqueName("QA_MOD_PARENT");
        String child = DbTestSupport.uniqueName("QA_MOD_CHILD");
        String fkName = DbTestSupport.uniqueName("QA_MOD_FK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(isbn char(10) primary key)");
        jdbc.executeUpdate(connection, "create table " + child + "(id integer)");
        jdbc.executeUpdate(connection,
                "alter table " + child + " add column (isbn char(10) constraint " + fkName + " references " + parent + "(isbn))");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "insert into " + child + "(id, isbn) values(1, 'NOTFOUND')"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_081_002 modify constraint by dropping foreign key")
    void tc081002ModifyConstraintDropForeignKey() {
        String parent = DbTestSupport.uniqueName("QA_DROP_FK_PARENT");
        String child = DbTestSupport.uniqueName("QA_DROP_FK_CHILD");
        String fkName = DbTestSupport.uniqueName("QA_DROP_FK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(isbn char(10) primary key)");
        jdbc.executeUpdate(connection, "create table " + child + "(id integer, isbn char(10), constraint " + fkName + " foreign key(isbn) references " + parent + "(isbn))");
        jdbc.executeUpdate(connection, "alter table " + child + " drop constraint " + fkName);
        jdbc.executeUpdate(connection, "insert into " + child + "(id, isbn) values(1, 'NOTFOUND')");

        assertThat(jdbc.query(connection, "select * from " + child).size()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_081_003 modify constraint by dropping primary key")
    void tc081003ModifyConstraintDropPrimaryKey() {
        String tableName = DbTestSupport.uniqueName("QA_DROP_PK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(isbn char(10) primary key)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop primary key");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values('0000000001')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values('0000000001')");

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isEqualTo(2);
    }

    @Test
    @DisplayName("TC_082_001 rename constraint")
    void tc082001RenameConstraint() {
        String tableName = DbTestSupport.uniqueName("QA_REN_CONS");
        String oldName = DbTestSupport.uniqueName("QA_OLD_CONS");
        String newName = DbTestSupport.uniqueName("QA_NEW_CONS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(bno integer constraint " + oldName + " unique)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " rename constraint " + oldName + " to " + newName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop constraint " + newName);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isEqualTo(2);
    }
}
