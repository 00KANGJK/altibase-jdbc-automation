package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionedTableJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_060_001 create a range partitioned table")
    void tc060001CreateRangePartitionedTable() {
        String tableName = DbTestSupport.uniqueName("QA_RANGE_PART");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createRangePartitionTable(tableName);

        assertThat(partitionCount(tableName)).isEqualTo(4);
        assertThat(partitionExists(tableName, "PART_1")).isTrue();
        assertThat(partitionExists(tableName, "PART_DEF")).isTrue();
    }

    @Test
    @DisplayName("TC_061_001 split a range partition")
    void tc061001SplitRangePartition() {
        String tableName = DbTestSupport.uniqueName("QA_RANGE_SPLIT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createRangePartitionTable(tableName);
        jdbc.executeUpdate(connection,
                "alter table " + tableName + " split partition part_2 at(to_date('15-FEB-2006')) " +
                        "into(partition part_2, partition part_4)");

        assertThat(partitionCount(tableName)).isEqualTo(5);
        assertThat(partitionExists(tableName, "PART_4")).isTrue();
    }

    @Test
    @DisplayName("TC_062_001 drop a specific range partition")
    void tc062001DropRangePartition() {
        String tableName = DbTestSupport.uniqueName("QA_RANGE_DROP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createRangePartitionTable(tableName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop partition part_2");

        assertThat(partitionCount(tableName)).isEqualTo(3);
        assertThat(partitionExists(tableName, "PART_2")).isFalse();
    }

    @Test
    @DisplayName("TC_063_001 merge range partitions")
    void tc063001MergeRangePartitions() {
        String tableName = DbTestSupport.uniqueName("QA_RANGE_MERGE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createRangePartitionTable(tableName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " merge partitions part_2, part_3 into partition part_3");

        assertThat(partitionCount(tableName)).isEqualTo(3);
        assertThat(partitionExists(tableName, "PART_2")).isFalse();
        assertThat(partitionExists(tableName, "PART_3")).isTrue();
    }

    @Test
    @DisplayName("TC_064_001 rename a partition")
    void tc064001RenamePartition() {
        String tableName = DbTestSupport.uniqueName("QA_LIST_RENAME");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createListPartitionTable(tableName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " rename partition part_1 to part_1_list");

        assertThat(partitionExists(tableName, "PART_1")).isFalse();
        assertThat(partitionExists(tableName, "PART_1_LIST")).isTrue();
    }

    @Test
    @DisplayName("TC_065_001 truncate a specific partition")
    void tc065001TruncateSpecificPartition() {
        String tableName = DbTestSupport.uniqueName("QA_RANGE_TRUNC");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createRangePartitionTable(tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('10-JAN-2006'), 1, 'SEOUL', 100)");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");

        jdbc.executeUpdate(connection, "alter table " + tableName + " truncate partition part_1");

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
    }

    @Test
    @DisplayName("TC_066_001 create a list partitioned table")
    void tc066001CreateListPartitionedTable() {
        String tableName = DbTestSupport.uniqueName("QA_LIST_PART");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createListPartitionTable(tableName);

        assertThat(partitionCount(tableName)).isEqualTo(4);
        assertThat(partitionExists(tableName, "PART_2")).isTrue();
        assertThat(partitionExists(tableName, "PART_DEF")).isTrue();
    }

    @Test
    @DisplayName("TC_067_001 split a list partition")
    void tc067001SplitListPartition() {
        String tableName = DbTestSupport.uniqueName("QA_LIST_SPLIT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createListPartitionTable(tableName);
        jdbc.executeUpdate(connection,
                "alter table " + tableName + " split partition part_2 values('BUSAN') into(partition part_4, partition part_2)");

        assertThat(partitionCount(tableName)).isEqualTo(5);
        assertThat(partitionExists(tableName, "PART_4")).isTrue();
    }

    @Test
    @DisplayName("TC_068_001 drop a specific list partition")
    void tc068001DropListPartition() {
        String tableName = DbTestSupport.uniqueName("QA_LIST_DROP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createListPartitionTable(tableName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " drop partition part_2");

        assertThat(partitionCount(tableName)).isEqualTo(3);
        assertThat(partitionExists(tableName, "PART_2")).isFalse();
    }

    @Test
    @DisplayName("TC_069_001 merge list partitions")
    void tc069001MergeListPartitions() {
        String tableName = DbTestSupport.uniqueName("QA_LIST_MERGE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createListPartitionTable(tableName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " merge partitions part_2, part_3 into partition part_3");

        assertThat(partitionCount(tableName)).isEqualTo(3);
        assertThat(partitionExists(tableName, "PART_2")).isFalse();
        assertThat(partitionExists(tableName, "PART_3")).isTrue();
    }

    @Test
    @DisplayName("TC_070_001 create a hash partitioned table")
    void tc070001CreateHashPartitionedTable() {
        String tableName = DbTestSupport.uniqueName("QA_HASH_PART");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createHashPartitionTable(tableName);

        assertThat(partitionCount(tableName)).isEqualTo(3);
        assertThat(partitionExists(tableName, "P1")).isTrue();
        assertThat(partitionExists(tableName, "P3")).isTrue();
    }

    @Test
    @DisplayName("TC_071_001 add a partition to a hash partitioned table")
    void tc071001AddPartitionToHashTable() {
        String tableName = DbTestSupport.uniqueName("QA_HASH_ADD");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createHashPartitionTable(tableName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " add partition part_5");

        assertThat(partitionCount(tableName)).isEqualTo(4);
        assertThat(partitionExists(tableName, "PART_5")).isTrue();
    }

    @Test
    @DisplayName("TC_072_001 coalesce partitions reduces the partition count")
    void tc072001CoalescePartition() {
        String tableName = DbTestSupport.uniqueName("QA_HASH_COALESCE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createHashPartitionTable(tableName);
        jdbc.executeUpdate(connection, "alter table " + tableName + " add partition part_4");
        assertThat(partitionCount(tableName)).isEqualTo(4);

        jdbc.executeUpdate(connection, "alter table " + tableName + " coalesce partition");

        assertThat(partitionCount(tableName)).isEqualTo(3);
    }

    @Test
    @DisplayName("Additional boundary case: range partitions route boundary and default rows")
    void rangePartitionsRouteBoundaryAndDefaultRows() {
        String tableName = DbTestSupport.uniqueName("QA_RANGE_ROUTE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createRangePartitionTable(tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-31','YYYY-MM-DD'), 1, 'SEOUL', 100)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-02-01','YYYY-MM-DD'), 2, 'BUSAN', 200)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-03-15','YYYY-MM-DD'), 3, 'DAEJUN', 300)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-04-01','YYYY-MM-DD'), 4, 'JEJU', 400)");

        assertThat(partitionRowCount(tableName, "PART_1")).isEqualTo(1);
        assertThat(partitionRowCount(tableName, "PART_2")).isEqualTo(1);
        assertThat(partitionRowCount(tableName, "PART_3")).isEqualTo(1);
        assertThat(partitionRowCount(tableName, "PART_DEF")).isEqualTo(1);
        assertThat(jdbc.queryForString(connection, "select sum(amount) from " + tableName)).isEqualTo("1000");
    }

    @Test
    @DisplayName("Additional boundary case: list partitions route configured and default values")
    void listPartitionsRouteConfiguredAndDefaultValues() {
        String tableName = DbTestSupport.uniqueName("QA_LIST_ROUTE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createListPartitionTable(tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-01','YYYY-MM-DD'), 1, 'SEOUL', 100)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-02','YYYY-MM-DD'), 2, 'BUSAN', 200)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-03','YYYY-MM-DD'), 3, 'DAEJUN', 300)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-04','YYYY-MM-DD'), 4, 'JEJU', 400)");

        assertThat(partitionRowCount(tableName, "PART_1")).isEqualTo(1);
        assertThat(partitionRowCount(tableName, "PART_2")).isEqualTo(1);
        assertThat(partitionRowCount(tableName, "PART_3")).isEqualTo(1);
        assertThat(partitionRowCount(tableName, "PART_DEF")).isEqualTo(1);
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName + " where sales_city = 'JEJU'")).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional boundary case: split range partition preserves rows and redistributes them")
    void splitRangePartitionPreservesRowsAndRedistributesThem() {
        String tableName = DbTestSupport.uniqueName("QA_RANGE_SPLIT_ROWS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createRangePartitionTable(tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-02-10','YYYY-MM-DD'), 1, 'BUSAN', 100)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-02-20','YYYY-MM-DD'), 2, 'BUSAN', 200)");

        jdbc.executeUpdate(connection,
                "alter table " + tableName + " split partition part_2 at(to_date('15-FEB-2006')) " +
                        "into(partition part_2, partition part_4)");

        assertThat(partitionRowCount(tableName, "PART_2")).isEqualTo(1);
        assertThat(partitionRowCount(tableName, "PART_4")).isEqualTo(1);
        assertThat(jdbc.queryForString(connection, "select sum(amount) from " + tableName)).isEqualTo("300");
    }

    @Test
    @DisplayName("Additional boundary case: merge list partitions preserves rows")
    void mergeListPartitionsPreservesRows() {
        String tableName = DbTestSupport.uniqueName("QA_LIST_MERGE_ROWS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createListPartitionTable(tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-01','YYYY-MM-DD'), 1, 'BUSAN', 100)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-02','YYYY-MM-DD'), 2, 'JUNJU', 200)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-03','YYYY-MM-DD'), 3, 'CHUNGJU', 300)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(to_date('2006-01-04','YYYY-MM-DD'), 4, 'DAEJUN', 400)");

        jdbc.executeUpdate(connection, "alter table " + tableName + " merge partitions part_2, part_3 into partition part_3");

        assertThat(partitionExists(tableName, "PART_2")).isFalse();
        assertThat(partitionRowCount(tableName, "PART_3")).isEqualTo(4);
        assertThat(jdbc.queryForString(connection, "select sum(amount) from " + tableName)).isEqualTo("1000");
    }

    @Test
    @DisplayName("Additional boundary case: hash partition coalesce preserves table rows")
    void hashPartitionCoalescePreservesRows() {
        String tableName = DbTestSupport.uniqueName("QA_HASH_COALESCE_ROWS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createHashPartitionTable(tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(10, 'P-001', 'KR', to_date('2006-01-01','YYYY-MM-DD'), 100)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(20, 'P-002', 'US', to_date('2006-01-02','YYYY-MM-DD'), 200)");
        jdbc.executeUpdate(connection, "alter table " + tableName + " add partition part_4");

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("2");

        jdbc.executeUpdate(connection, "alter table " + tableName + " coalesce partition");

        assertThat(partitionCount(tableName)).isEqualTo(3);
        assertThat(jdbc.queryForString(connection, "select sum(amount) from " + tableName)).isEqualTo("300");
    }

    @Test
    @DisplayName("Additional negative case: partition rename rejects duplicate names")
    void partitionRenameRejectsDuplicateNames() {
        String tableName = DbTestSupport.uniqueName("QA_PART_DUP_RENAME");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createListPartitionTable(tableName);

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "alter table " + tableName + " rename partition part_2 to part_1"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(partitionExists(tableName, "PART_1")).isTrue();
        assertThat(partitionExists(tableName, "PART_2")).isTrue();
    }

    private void createRangePartitionTable(String tableName) {
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

    private void createListPartitionTable(String tableName) {
        jdbc.executeUpdate(connection,
                "create table " + tableName + " (" +
                        "sales_date date, sales_id number, sales_city varchar(20), amount number" +
                        ") partition by list(sales_city) (" +
                        "partition part_1 values('SEOUL', 'INCHEON'), " +
                        "partition part_2 values('BUSAN', 'JUNJU'), " +
                        "partition part_3 values('CHUNGJU', 'DAEJUN'), " +
                        "partition part_def values default" +
                        ") tablespace SYS_TBS_MEMORY");
    }

    private void createHashPartitionTable(String tableName) {
        jdbc.executeUpdate(connection,
                "create table " + tableName + " (" +
                        "dept_no number, part_no varchar(20), country varchar(20), sales_date date, amount number" +
                        ") partition by hash(part_no) (" +
                        "partition p1, partition p2, partition p3" +
                        ")");
    }

    private int partitionCount(String tableName) {
        return Integer.parseInt(jdbc.queryForString(connection,
                "select count(*) from system_.sys_table_partitions_ p join system_.sys_tables_ t " +
                        "on p.table_id = t.table_id and p.user_id = t.user_id " +
                        "where t.table_name = '" + tableName + "'"));
    }

    private boolean partitionExists(String tableName, String partitionName) {
        return jdbc.exists(connection,
                "select p.partition_name from system_.sys_table_partitions_ p join system_.sys_tables_ t " +
                        "on p.table_id = t.table_id and p.user_id = t.user_id " +
                        "where t.table_name = '" + tableName + "' and p.partition_name = '" + partitionName + "'");
    }

    private int partitionRowCount(String tableName, String partitionName) {
        return Integer.parseInt(jdbc.queryForString(
                connection,
                "select count(*) from " + tableName + " partition(" + partitionName + ")"
        ));
    }
}
