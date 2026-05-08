package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaObjectLifecycleJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional manual case: COMMENT accepts table and column comments")
    void commentOnTableAndColumnSucceeds() {
        String tableName = DbTestSupport.uniqueName("QA_COMMENT_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 varchar(20))");
        jdbc.executeUpdate(connection, "comment on table " + tableName + " is 'qa table comment'");
        jdbc.executeUpdate(connection, "comment on column " + tableName + ".c2 is 'qa column comment'");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'A')");

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1"))
                .isEqualTo("A");
    }

    @Test
    @DisplayName("Additional negative case: COMMENT on a missing table fails")
    void commentOnMissingTableFails() {
        String tableName = DbTestSupport.uniqueName("QA_COMMENT_MISSING");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "comment on table " + tableName + " is 'missing'"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional manual case: RENAME TABLE updates metadata and preserves data")
    void renameTableUpdatesMetadataAndPreservesData() {
        String originalName = DbTestSupport.uniqueName("QA_RENAME_SRC");
        String renamedName = DbTestSupport.uniqueName("QA_RENAME_DST");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, renamedName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, originalName));

        jdbc.executeUpdate(connection, "create table " + originalName + "(c1 integer primary key, c2 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + originalName + " values(1, 'A')");
        jdbc.executeUpdate(connection, "rename " + originalName + " to " + renamedName);

        assertThat(DbTestSupport.tableExists(connection, null, originalName)).isFalse();
        assertThat(DbTestSupport.tableExists(connection, null, renamedName)).isTrue();
        assertThat(jdbc.queryForString(connection, "select c2 from " + renamedName + " where c1 = 1"))
                .isEqualTo("A");
    }

    @Test
    @DisplayName("Additional negative case: RENAME TABLE to an existing table fails")
    void renameTableToExistingNameFails() {
        String sourceName = DbTestSupport.uniqueName("QA_RENAME_CONFLICT_SRC");
        String targetName = DbTestSupport.uniqueName("QA_RENAME_CONFLICT_DST");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, targetName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, sourceName));

        jdbc.executeUpdate(connection, "create table " + sourceName + "(c1 integer)");
        jdbc.executeUpdate(connection, "create table " + targetName + "(c1 integer)");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "rename " + sourceName + " to " + targetName))
                .isInstanceOf(IllegalStateException.class);
        assertThat(DbTestSupport.tableExists(connection, null, sourceName)).isTrue();
        assertThat(DbTestSupport.tableExists(connection, null, targetName)).isTrue();
    }

    @Test
    @DisplayName("Additional manual case: TRUNCATE TABLE removes rows as a non-rollback DDL")
    void truncateTableRemovesRowsAndCannotBeRolledBack() {
        String tableName = DbTestSupport.uniqueName("QA_TRUNCATE_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.begin(connection);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
            jdbc.executeUpdate(connection, "truncate table " + tableName);
            jdbc.rollback(connection);
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (Exception ignored) {
            }
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
    }

    @Test
    @DisplayName("Additional negative case: TRUNCATE TABLE rejects a referenced parent table")
    void truncateReferencedParentTableFails() {
        String parentName = DbTestSupport.uniqueName("QA_TRUNC_PARENT");
        String childName = DbTestSupport.uniqueName("QA_TRUNC_CHILD");
        String constraintName = DbTestSupport.uniqueName("QA_TRUNC_FK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, childName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parentName));

        jdbc.executeUpdate(connection, "create table " + parentName + "(id integer primary key)");
        jdbc.executeUpdate(connection, "create table " + childName + "(id integer primary key, parent_id integer, " +
                "constraint " + constraintName + " foreign key(parent_id) references " + parentName + "(id))");
        jdbc.executeUpdate(connection, "insert into " + parentName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + childName + " values(10, 1)");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "truncate table " + parentName))
                .isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForString(connection, "select count(*) from " + parentName)).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional negative case: PURGE TABLE for a missing table fails")
    void purgeMissingTableFails() {
        String tableName = DbTestSupport.uniqueName("QA_PURGE_MISSING");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "purge table " + tableName))
                .isInstanceOf(IllegalStateException.class);
    }
}
