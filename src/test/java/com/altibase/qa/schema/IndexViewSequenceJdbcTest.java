package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexViewSequenceJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_083_001 create table with inline primary and unique index")
    void tc083001CreateTableWithInlineIndexes() {
        String tableName = DbTestSupport.uniqueName("QA_IDX_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 integer unique)");

        assertThat(hasIndexOnColumn(tableName, "C1")).isTrue();
        assertThat(hasIndexOnColumn(tableName, "C2")).isTrue();
    }

    @Test
    @DisplayName("TC_083_002 create ascending and descending index")
    void tc083002CreateAscDescIndex() {
        String tableName = DbTestSupport.uniqueName("QA_SORT_IDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_SORT_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(c1 asc, c2 desc)");

        assertThat(jdbc.exists(connection,
                "select index_name from system_.sys_indices_ where index_name = '" + indexName + "'"))
                .isTrue();
    }

    @Test
    @DisplayName("TC_083_003 create unique index")
    void tc083003CreateUniqueIndex() {
        String tableName = DbTestSupport.uniqueName("QA_UIDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_UIDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");
        jdbc.executeUpdate(connection, "create unique index " + indexName + " on " + tableName + "(c2)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'A')");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'A')"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_083_004 create a direct key index")
    void tc083004CreateDirectKeyIndex() {
        String tableName = DbTestSupport.uniqueName("QA_DIRECTKEY_TB");
        String indexName = DbTestSupport.uniqueName("QA_DIRECTKEY_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(c1) directkey");

        assertThat(jdbc.queryForString(connection,
                "select is_directkey from system_.sys_indices_ where index_name = '" + indexName + "'")).isEqualTo("T");
    }

    @Test
    @DisplayName("TC_084_001 alter an index to enable direct key")
    void tc084001AlterIndexToDirectKey() {
        String tableName = DbTestSupport.uniqueName("QA_ALT_DIRECTKEY_TB");
        String indexName = DbTestSupport.uniqueName("QA_ALT_DIRECTKEY_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(c1)");
        jdbc.executeUpdate(connection, "alter index " + indexName + " directkey");

        assertThat(jdbc.queryForString(connection,
                "select is_directkey from system_.sys_indices_ where index_name = '" + indexName + "'")).isEqualTo("T");
    }

    @Test
    @DisplayName("TC_084_002 alter a direct key index back to a normal index")
    void tc084002AlterDirectKeyIndexToNormal() {
        String tableName = DbTestSupport.uniqueName("QA_ALT_NORMAL_TB");
        String indexName = DbTestSupport.uniqueName("QA_ALT_NORMAL_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(c1) directkey");
        jdbc.executeUpdate(connection, "alter index " + indexName + " directkey off");

        assertThat(jdbc.queryForString(connection,
                "select is_directkey from system_.sys_indices_ where index_name = '" + indexName + "'")).isEqualTo("F");
    }

    @Test
    @DisplayName("TC_084_003 rename an index")
    void tc084003RenameIndex() {
        String tableName = DbTestSupport.uniqueName("QA_REN084_IDX_TB");
        String oldIndexName = DbTestSupport.uniqueName("QA_REN084_OLD");
        String newIndexName = DbTestSupport.uniqueName("QA_REN084_NEW");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        jdbc.executeUpdate(connection, "create index " + oldIndexName + " on " + tableName + "(c1 asc, c2 desc)");
        jdbc.executeUpdate(connection, "alter index " + oldIndexName + " rename to " + newIndexName);

        assertThat(jdbc.exists(connection,
                "select index_name from system_.sys_indices_ where index_name = '" + oldIndexName + "'"))
                .isFalse();
        assertThat(jdbc.exists(connection,
                "select index_name from system_.sys_indices_ where index_name = '" + newIndexName + "'"))
                .isTrue();
    }

    @Test
    @DisplayName("TC_085_001 drop index")
    void tc085001DropIndex() {
        String tableName = DbTestSupport.uniqueName("QA_DROP_IDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_DROP_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 int)");
        jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(c1)");
        jdbc.executeUpdate(connection, "drop index " + indexName);

        assertThat(jdbc.exists(connection,
                "select index_name from system_.sys_indices_ where index_name = '" + indexName + "'"))
                .isFalse();
    }

    @Test
    @DisplayName("TC_086_001 create a local index on a partitioned table")
    void tc086001CreateLocalIndexOnPartitionedTable() {
        String tableName = DbTestSupport.uniqueName("QA_LOCAL_IDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_LOCAL_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createRangePartitionedIndexTable(tableName);
        jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(sales_date) local");

        assertThat(jdbc.queryForString(connection,
                "select is_partitioned from system_.sys_indices_ where index_name = '" + indexName + "'")).isEqualTo("T");
        assertThat(indexPartitionCount(indexName)).isGreaterThanOrEqualTo(4);
    }

    @Test
    @DisplayName("TC_090_001 rename index")
    void tc090001RenameIndex() {
        String tableName = DbTestSupport.uniqueName("QA_REN_IDX_TB");
        String oldIndexName = DbTestSupport.uniqueName("QA_OLD_IDX");
        String newIndexName = DbTestSupport.uniqueName("QA_NEW_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        jdbc.executeUpdate(connection, "create index " + oldIndexName + " on " + tableName + "(c1 asc, c2 desc)");
        jdbc.executeUpdate(connection, "alter index " + oldIndexName + " rename to " + newIndexName);

        assertThat(jdbc.exists(connection,
                "select index_name from system_.sys_indices_ where index_name = '" + oldIndexName + "'"))
                .isFalse();
        assertThat(jdbc.exists(connection,
                "select index_name from system_.sys_indices_ where index_name = '" + newIndexName + "'"))
                .isTrue();
    }

    @Test
    @DisplayName("TC_091_001 drop index")
    void tc091001DropIndex() {
        String tableName = DbTestSupport.uniqueName("QA_DROP2_IDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_DROP2_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(c1)");
        jdbc.executeUpdate(connection, "drop index " + indexName);

        assertThat(jdbc.exists(connection,
                "select index_name from system_.sys_indices_ where index_name = '" + indexName + "'"))
                .isFalse();
    }

    @Test
    @DisplayName("TC_092_001 create view from tables")
    void tc092001CreateView() {
        String src1 = DbTestSupport.uniqueName("QA_VIEW_SRC1");
        String src2 = DbTestSupport.uniqueName("QA_VIEW_SRC2");
        String viewName = DbTestSupport.uniqueName("QA_VIEW");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, src2));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, src1));

        jdbc.executeUpdate(connection, "create table " + src1 + "(c1 int, v1 varchar(20))");
        jdbc.executeUpdate(connection, "create table " + src2 + "(c2 int, v2 char(10))");
        jdbc.executeUpdate(connection, "insert into " + src1 + " values(1, 'A')");
        jdbc.executeUpdate(connection, "insert into " + src2 + " values(2, 'B')");
        jdbc.executeUpdate(connection, "create view " + viewName + " as select * from " + src1 + ", " + src2);

        assertThat(jdbc.query(connection, "select * from " + viewName).size()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_092_002 create or replace view")
    void tc092002CreateOrReplaceView() {
        String src1 = DbTestSupport.uniqueName("QA_VIEW_REP_SRC1");
        String src2 = DbTestSupport.uniqueName("QA_VIEW_REP_SRC2");
        String viewName = DbTestSupport.uniqueName("QA_VIEW_REP");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, src2));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, src1));

        jdbc.executeUpdate(connection, "create table " + src1 + "(c1 int, v1 varchar(20))");
        jdbc.executeUpdate(connection, "create table " + src2 + "(c2 int, v2 char(10))");
        jdbc.executeUpdate(connection, "insert into " + src1 + " values(1, 'A')");
        jdbc.executeUpdate(connection, "insert into " + src2 + " values(2, 'B')");
        jdbc.executeUpdate(connection, "create view " + viewName + " as select c1, v1 from " + src1);
        jdbc.executeUpdate(connection, "create or replace view " + viewName + " as select c1, v2 from " + src1 + ", " + src2);

        assertThat(jdbc.queryForString(connection, "select v2 from " + viewName + " where c1 = 1").trim()).isEqualTo("B");
    }

    @Test
    @DisplayName("TC_092_003 create read only view")
    void tc092003CreateReadOnlyView() {
        String src = DbTestSupport.uniqueName("QA_VIEW_RO_SRC");
        String viewName = DbTestSupport.uniqueName("QA_VIEW_RO");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, src));

        jdbc.executeUpdate(connection, "create table " + src + "(c1 int)");
        jdbc.executeUpdate(connection, "insert into " + src + " values(1)");
        jdbc.executeUpdate(connection, "create or replace view " + viewName + " as select c1 from " + src + " with read only");

        assertThat(jdbc.queryForString(connection, "select c1 from " + viewName)).isEqualTo("1");
        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + viewName + " values(2)"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_092_004 create force view without base-table privilege")
    void tc092004CreateForceViewWithoutPrivilege() throws Exception {
        String owner = createManagedUser("QA_FORCE_VIEW_OWNER");
        String consumer = createManagedUser("QA_FORCE_VIEW_USER");
        String viewName = DbTestSupport.uniqueName("QA_FORCE_VIEW");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table FORCE_VIEW_SRC(c1 int)");
            jdbc.executeUpdate(conn, "insert into FORCE_VIEW_SRC values(1)");
        });

        withUserConnection(consumer, conn ->
                jdbc.executeUpdate(conn, "create or replace force view " + viewName + " as select c1 from " + owner + ".FORCE_VIEW_SRC"));

        assertThat(DbTestSupport.viewExists(connection, consumer, viewName)).isTrue();
        assertThatThrownBy(() -> withUserConnection(consumer, conn -> jdbc.queryForString(conn, "select c1 from " + viewName)))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "grant select on " + owner + ".FORCE_VIEW_SRC to " + consumer);
        withUserConnection(consumer, conn -> jdbc.executeUpdate(conn, "alter view " + viewName + " compile"));
        withUserConnection(consumer, conn -> assertThat(jdbc.queryForString(conn, "select c1 from " + viewName)).isEqualTo("1"));
    }

    @Test
    @DisplayName("TC_093_001 alter view compile refreshes metadata after base-table changes")
    void tc093001AlterViewCompile() {
        String src = DbTestSupport.uniqueName("QA_VIEW_COMPILE_SRC");
        String viewName = DbTestSupport.uniqueName("QA_VIEW_COMPILE");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, src));

        jdbc.executeUpdate(connection, "create table " + src + "(c1 int)");
        jdbc.executeUpdate(connection, "insert into " + src + " values(1)");
        jdbc.executeUpdate(connection, "create view " + viewName + " as select * from " + src);

        assertThat(jdbc.query(connection, "select * from " + viewName).columns()).containsExactly("C1");

        jdbc.executeUpdate(connection, "alter table " + src + " add column (c2 int)");
        jdbc.executeUpdate(connection, "alter view " + viewName + " compile");

        QueryResult result = jdbc.query(connection, "select * from " + viewName);
        assertThat(result.columns()).containsExactly("C1", "C2");
    }

    @Test
    @DisplayName("TC_094_001 drop view")
    void tc094001DropView() {
        String tableName = DbTestSupport.uniqueName("QA_DROP_VIEW_TB");
        String viewName = DbTestSupport.uniqueName("QA_DROP_VIEW");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "create view " + viewName + " as select c1 from " + tableName);
        jdbc.executeUpdate(connection, "drop view " + viewName);

        assertThat(DbTestSupport.viewExists(connection, null, viewName)).isFalse();
    }

    @Test
    @DisplayName("TC_095_001 create materialized view")
    void tc095001CreateMaterializedView() {
        String tableName = DbTestSupport.uniqueName("QA_MV_SRC");
        String viewName = DbTestSupport.uniqueName("QA_MV");
        registerCleanup(() -> dropMaterializedViewQuietly(viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
        jdbc.executeUpdate(connection, "create materialized view " + viewName + " as select * from " + tableName);

        assertThat(jdbc.query(connection, "select * from " + viewName).size()).isEqualTo(2);
    }

    @Test
    @DisplayName("TC_095_002 materialized view MAXROWS enforces the row limit")
    void tc095002CreateMaterializedViewWithMaxRows() {
        String tableName = DbTestSupport.uniqueName("QA_MV_MAX_SRC");
        String blockedView = DbTestSupport.uniqueName("QA_MV_MAX_BLOCK");
        String createdView = DbTestSupport.uniqueName("QA_MV_MAX_OK");
        registerCleanup(() -> dropMaterializedViewQuietly(blockedView));
        registerCleanup(() -> dropMaterializedViewQuietly(createdView));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        for (int value = 1; value <= 11; value++) {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(" + value + ")");
        }

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "create materialized view " + blockedView + " maxrows 10 as select * from " + tableName))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "create materialized view " + createdView + " maxrows 11 as select * from " + tableName);
        assertThat(jdbc.queryForString(connection, "select count(*) from " + createdView)).isEqualTo("11");
    }

    @Test
    @DisplayName("TC_095_003 create a materialized view in a specified tablespace")
    void tc095003CreateMaterializedViewInSpecifiedTablespace() {
        String tablespaceName = DbTestSupport.uniqueName("QA_MV_TBS");
        String tableName = DbTestSupport.uniqueName("QA_MV_TBS_SRC");
        String viewName = DbTestSupport.uniqueName("QA_MV_TBS_VIEW");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));
        registerCleanup(() -> dropMaterializedViewQuietly(viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create memory tablespace " + tablespaceName + " size 8M");
        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        for (int value = 1; value <= 11; value++) {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(" + value + ")");
        }
        jdbc.executeUpdate(connection,
                "create materialized view " + viewName + " tablespace " + tablespaceName + " build immediate as select c1 from " + tableName);

        assertThat(jdbc.queryForString(connection,
                "select tbs_name from system_.sys_tables_ where table_name = '" + viewName + "'")).isEqualTo(tablespaceName);
    }

    @Test
    @DisplayName("Additional negative case: materialized views remain read only")
    void materializedViewIsReadOnly() {
        String tableName = DbTestSupport.uniqueName("QA_MV_RO_SRC");
        String viewName = DbTestSupport.uniqueName("QA_MV_RO");
        registerCleanup(() -> dropMaterializedViewQuietly(viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "create materialized view " + viewName + " as select * from " + tableName);

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + viewName + " values(2)"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: local indexes are rejected on non-partitioned tables")
    void localIndexOnNonPartitionedTableFails() {
        String tableName = DbTestSupport.uniqueName("QA_LOCAL_IDX_FAIL_TB");
        String indexName = DbTestSupport.uniqueName("QA_LOCAL_IDX_FAIL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 date)");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(c1) local"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: a normal view cannot be created from a missing base table")
    void createViewWithoutForceFailsWhenBaseTableIsMissing() {
        String viewName = DbTestSupport.uniqueName("QA_VIEW_MISSING");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, viewName));

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "create view " + viewName + " as select c1 from MISSING_BASE_TABLE"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_096_001 refresh complete on demand updates the materialized view only when requested")
    void tc096001RefreshMaterializedViewOnDemand() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_MV_REFRESH_SRC");
        String viewName = DbTestSupport.uniqueName("QA_MV_REFRESH");
        registerCleanup(() -> dropMaterializedViewQuietly(viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "create materialized view " + viewName + " build immediate refresh complete on demand as select * from " + tableName);

        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + viewName)).isEqualTo("1");

        refreshMaterializedView("SYS", viewName);
        assertThat(jdbc.queryForString(connection, "select count(*) from " + viewName)).isEqualTo("2");
    }

    @Test
    @DisplayName("TC_097_001 drop materialized view")
    void tc097001DropMaterializedView() {
        String tableName = DbTestSupport.uniqueName("QA_DROP_MV_SRC");
        String viewName = DbTestSupport.uniqueName("QA_DROP_MV");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "create materialized view " + viewName + " as select * from " + tableName);
        jdbc.executeUpdate(connection, "drop materialized view " + viewName);

        assertThatThrownBy(() -> jdbc.queryForString(connection, "select count(*) from " + viewName))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_098_001 create sequence")
    void tc098001CreateSequence() {
        String sequenceName = DbTestSupport.uniqueName("QA_SEQ");
        registerCleanup(() -> {
            try {
                jdbc.executeUpdate(connection, "drop sequence " + sequenceName);
            } catch (Exception ignored) {
            }
        });

        jdbc.executeUpdate(connection, "create sequence " + sequenceName + " start with 13 increment by 3 minvalue 0 nomaxvalue");

        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("13");
    }

    @Test
    @DisplayName("TC_098_002 create cycle sequence")
    void tc098002CreateCycleSequence() {
        String sequenceName = DbTestSupport.uniqueName("QA_CYCLE_SEQ");
        registerCleanup(() -> dropSequenceQuietly(sequenceName));

        jdbc.executeUpdate(connection, "create sequence " + sequenceName + " start with 1 increment by 50 minvalue 1 maxvalue 100 cycle");

        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("51");
        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional negative case: a nocycle sequence stops at MAXVALUE")
    void noCycleSequenceStopsAtMaxValue() {
        String sequenceName = DbTestSupport.uniqueName("QA_NOCYCLE_SEQ");
        registerCleanup(() -> dropSequenceQuietly(sequenceName));

        jdbc.executeUpdate(connection,
                "create sequence " + sequenceName + " start with 1 increment by 1 minvalue 1 maxvalue 2 nocycle");

        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("2");
        assertThatThrownBy(() ->
                jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_098_003 create a sequence with enable sync table")
    void tc098003CreateSequenceWithSyncTable() {
        String sequenceName = DbTestSupport.uniqueName("QA_SYNC_CREATE_SEQ");
        registerCleanup(() -> dropSequenceQuietly(sequenceName));

        jdbc.executeUpdate(connection,
                "create sequence " + sequenceName + " start with 1 increment by 1 cache 100 enable sync table");

        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("1");
        assertThat(jdbc.exists(connection,
                "select table_name from system_.sys_tables_ where table_name = '" + sequenceName + "$SEQ'"))
                .isTrue();
    }

    @Test
    @DisplayName("TC_099_001 alter sequence")
    void tc099001AlterSequence() {
        String sequenceName = DbTestSupport.uniqueName("QA_ALT_SEQ");
        registerCleanup(() -> {
            try {
                jdbc.executeUpdate(connection, "drop sequence " + sequenceName);
            } catch (Exception ignored) {
            }
        });

        jdbc.executeUpdate(connection, "create sequence " + sequenceName + " start with 13 increment by 3 minvalue 0 nomaxvalue");
        jdbc.executeUpdate(connection, "alter sequence " + sequenceName + " increment by 1 minvalue 0 maxvalue 100");

        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("13");
        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("14");
    }

    @Test
    @DisplayName("TC_099_002 alter sequence to NOMAXVALUE and NOMINVALUE")
    void tc099002AlterSequenceToNoBounds() {
        String sequenceName = DbTestSupport.uniqueName("QA_SEQ_NOBOUND");
        registerCleanup(() -> dropSequenceQuietly(sequenceName));

        jdbc.executeUpdate(connection, "create sequence " + sequenceName + " start with 1 increment by 50 minvalue 1 maxvalue 100 cycle");
        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("51");
        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("1");

        jdbc.executeUpdate(connection, "alter sequence " + sequenceName + " nomaxvalue nominvalue");

        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("51");
    }

    @Test
    @DisplayName("TC_099_003 alter sequence flush cache discards the in-memory cache")
    void tc099003AlterSequenceFlushCache() {
        String sequenceName = DbTestSupport.uniqueName("QA_FLUSH_SEQ");
        registerCleanup(() -> dropSequenceQuietly(sequenceName));

        jdbc.executeUpdate(connection, "create sequence " + sequenceName + " start with 1 increment by 1 cache 20");
        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("1");

        jdbc.executeUpdate(connection, "alter sequence " + sequenceName + " flush cache");

        assertThat(jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual")).isEqualTo("22");
    }

    @Test
    @DisplayName("TC_099_004 alter sequence enable sync table creates the sync table")
    void tc099004AlterSequenceEnableSyncTable() {
        String sequenceName = DbTestSupport.uniqueName("QA_SYNC_SEQ");
        registerCleanup(() -> dropSequenceQuietly(sequenceName));

        jdbc.executeUpdate(connection, "create sequence " + sequenceName + " start with 1 increment by 1");
        jdbc.executeUpdate(connection, "alter sequence " + sequenceName + " enable sync table");

        assertThat(jdbc.exists(connection,
                "select table_name from system_.sys_tables_ where table_name = '" + sequenceName + "$SEQ'"))
                .isTrue();
    }

    @Test
    @DisplayName("TC_100_001 drop sequence")
    void tc100001DropSequence() {
        String sequenceName = DbTestSupport.uniqueName("QA_DROP_SEQ");

        jdbc.executeUpdate(connection, "create sequence " + sequenceName + " start with 1 increment by 1");
        jdbc.executeUpdate(connection, "drop sequence " + sequenceName);

        assertThatThrownBy(() -> jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_101_001 private synonym can be used for DML on a base table")
    void tc101001CreateSynonymAndDml() {
        String owner = DbTestSupport.uniqueName("QA_SYN_OWNER");
        String synonymName = DbTestSupport.uniqueName("MY_DEPT");
        registerCleanup(() -> DbTestSupport.dropSynonymQuietly(jdbc, connection, synonymName));
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, owner));

        jdbc.executeUpdate(connection, "create user " + owner + " identified by " + owner);

        java.sql.Connection ownerConn = jdbc.open(owner, owner);
        try {
            jdbc.executeUpdate(ownerConn, "create table DEPT(deptno integer primary key, dname varchar(20))");
            jdbc.executeUpdate(ownerConn, "insert into DEPT values(10, 'SALES')");
        } finally {
            jdbc.closeQuietly(ownerConn);
        }

        jdbc.executeUpdate(connection, "create synonym " + synonymName + " for " + owner + ".DEPT");
        jdbc.executeUpdate(connection, "insert into " + synonymName + " values(20, 'ADMIN')");
        jdbc.executeUpdate(connection, "update " + synonymName + " set dname = 'HR' where deptno = 10");
        jdbc.executeUpdate(connection, "delete from " + synonymName + " where deptno = 20");

        assertThat(jdbc.queryForString(connection, "select dname from " + owner + ".DEPT where deptno = 10")).isEqualTo("HR");
        assertThat(jdbc.query(connection, "select * from " + owner + ".DEPT where deptno = 20").size()).isZero();
    }

    @Test
    @DisplayName("TC_102_001 private synonym can be dropped")
    void tc102001DropSynonym() {
        String synonymName = DbTestSupport.uniqueName("QA_DROP_SYN");

        jdbc.executeUpdate(connection, "create synonym " + synonymName + " for dual");
        jdbc.executeUpdate(connection, "drop synonym " + synonymName);

        assertThatThrownBy(() -> jdbc.queryForString(connection, "select count(*) from " + synonymName))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("TC_102_002 public synonym can be dropped")
    void tc102002DropPublicSynonym() {
        String synonymName = DbTestSupport.uniqueName("QA_DROP_PUB_SYN");
        registerCleanup(() -> DbTestSupport.dropPublicSynonymQuietly(jdbc, connection, synonymName));

        jdbc.executeUpdate(connection, "create public synonym " + synonymName + " for dual");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + synonymName)).isEqualTo("1");

        jdbc.executeUpdate(connection, "drop public synonym " + synonymName);

        assertThatThrownBy(() -> jdbc.queryForString(connection, "select count(*) from " + synonymName))
                .isInstanceOf(IllegalStateException.class);
    }

    private void createRangePartitionedIndexTable(String tableName) {
        jdbc.executeUpdate(connection,
                "create table " + tableName + " (" +
                        "sales_date date, sales_id number, sales_city varchar(20), amount number" +
                        ") partition by range(sales_date) (" +
                        "partition part_1 values less than (to_date('01-FEB-2006')), " +
                        "partition part_2 values less than (to_date('01-MAR-2006')), " +
                        "partition part_3 values less than (to_date('01-APR-2006')), " +
                        "partition part_def values default" +
                        ") tablespace SYS_TBS_MEMORY");
    }

    private int indexPartitionCount(String indexName) {
        return Integer.parseInt(jdbc.queryForString(connection,
                "select count(*) from system_.sys_index_partitions_ p join system_.sys_indices_ i " +
                        "on p.index_id = i.index_id and p.user_id = i.user_id " +
                        "where i.index_name = '" + indexName + "'"));
    }

    private boolean hasIndexOnColumn(String tableName, String columnName) {
        try (ResultSet rs = connection.getMetaData().getIndexInfo(null, null, tableName, false, false)) {
            Set<String> indexNames = new HashSet<>();
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String indexedColumn = rs.getString("COLUMN_NAME");
                if (indexName != null && indexedColumn != null && indexedColumn.equalsIgnoreCase(columnName)) {
                    indexNames.add(indexName);
                }
            }
            return !indexNames.isEmpty();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to inspect index metadata for " + tableName + "." + columnName, e);
        }
    }

    private String createManagedUser(String prefix) {
        String user = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);
        return user;
    }

    private void withUserConnection(String user, SqlWork work) throws Exception {
        Connection userConnection = jdbc.open(user, user);
        try {
            work.run(userConnection);
        } finally {
            jdbc.closeQuietly(userConnection);
        }
    }

    private void refreshMaterializedView(String owner, String viewName) throws Exception {
        try (CallableStatement cs = connection.prepareCall("{call refresh_materialized_view(?, ?)}")) {
            cs.setString(1, owner);
            cs.setString(2, viewName);
            cs.execute();
        }
    }

    private void dropMaterializedViewQuietly(String viewName) {
        try {
            jdbc.executeUpdate(connection, "drop materialized view " + viewName);
        } catch (Exception ignored) {
        }
    }

    private void dropSequenceQuietly(String sequenceName) {
        try {
            jdbc.executeUpdate(connection, "drop sequence " + sequenceName);
        } catch (Exception ignored) {
        }
    }

    @FunctionalInterface
    private interface SqlWork {
        void run(Connection connection) throws Exception;
    }
}
