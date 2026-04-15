package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TemporaryTableJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_047_001 create a temporary table in a volatile tablespace")
    void tc047001CreateTemporaryTableInVolatileTablespace() {
        String tablespaceName = DbTestSupport.uniqueName("QA_VOL_TBS_CREATE");
        String tableName = DbTestSupport.uniqueName("QA_TMP_CREATE");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create volatile data tablespace " + tablespaceName + " size 4M");
        jdbc.executeUpdate(connection,
                "create temporary table " + tableName + "(c1 int) on commit preserve rows tablespace " + tablespaceName);

        assertThat(DbTestSupport.tableExists(connection, null, tableName)).isTrue();
        assertThat(jdbc.queryForString(connection,
                "select tbs_name from system_.sys_tables_ where table_name = '" + tableName + "'")).isEqualTo(tablespaceName);
    }

    @Test
    @DisplayName("TC_075_001 alter temporary table add column")
    void tc075001AddColumnToTemporaryTable() {
        String tablespaceName = DbTestSupport.uniqueName("QA_VOL_TBS_ADD");
        String tableName = DbTestSupport.uniqueName("QA_TMP_ADD");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create volatile data tablespace " + tablespaceName + " size 4M");
        jdbc.executeUpdate(connection, "create temporary table " + tableName + "(c1 int) on commit preserve rows tablespace " + tablespaceName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " add column(v2 varchar(10))");

        assertThat(DbTestSupport.columnExists(connection, null, tableName, "V2")).isTrue();
    }

    @Test
    @DisplayName("TC_076_001 alter temporary table drop column")
    void tc076001DropColumnFromTemporaryTable() {
        String tablespaceName = DbTestSupport.uniqueName("QA_VOL_TBS_DROP");
        String tableName = DbTestSupport.uniqueName("QA_TMP_DROP_COL");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create volatile data tablespace " + tablespaceName + " size 4M");
        jdbc.executeUpdate(connection, "create temporary table " + tableName + "(c1 int, v2 varchar(10)) on commit preserve rows tablespace " + tablespaceName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop column v2");

        assertThat(DbTestSupport.columnExists(connection, null, tableName, "V2")).isFalse();
    }

    @Test
    @DisplayName("TC_077_001 drop temporary table")
    void tc077001DropTemporaryTable() {
        String tablespaceName = DbTestSupport.uniqueName("QA_VOL_TBS_TMP_DROP");
        String tableName = DbTestSupport.uniqueName("QA_TMP_DROP");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));

        jdbc.executeUpdate(connection, "create volatile data tablespace " + tablespaceName + " size 4M");
        jdbc.executeUpdate(connection, "create temporary table " + tableName + "(c1 int) tablespace " + tablespaceName);
        jdbc.executeUpdate(connection, "drop table " + tableName);

        assertThat(DbTestSupport.tableExists(connection, null, tableName)).isFalse();
    }
}
