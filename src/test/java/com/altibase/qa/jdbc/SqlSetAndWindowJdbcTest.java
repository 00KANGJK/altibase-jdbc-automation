package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlSetAndWindowJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional manual case: INTERSECT returns only distinct rows present in both inputs")
    void intersectReturnsDistinctCommonRows() {
        String leftTable = DbTestSupport.uniqueName("QA_INTERSECT_L");
        String rightTable = DbTestSupport.uniqueName("QA_INTERSECT_R");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, rightTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, leftTable));

        createSetOperatorFixture(leftTable, rightTable);

        QueryResult result = jdbc.query(
                connection,
                "select c1, c2 from " + leftTable + " intersect select c1, c2 from " + rightTable + " order by c1"
        );

        assertThat(result.size()).isEqualTo(1);
        assertThat(((Number) result.value(0, "C1")).intValue()).isEqualTo(2);
        assertThat(result.value(0, "C2")).isEqualTo("B");
    }

    @Test
    @DisplayName("Additional manual case: MINUS returns distinct rows present only in the left input")
    void minusReturnsDistinctLeftOnlyRows() {
        String leftTable = DbTestSupport.uniqueName("QA_MINUS_L");
        String rightTable = DbTestSupport.uniqueName("QA_MINUS_R");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, rightTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, leftTable));

        createSetOperatorFixture(leftTable, rightTable);

        QueryResult result = jdbc.query(
                connection,
                "select c1, c2 from " + leftTable + " minus select c1, c2 from " + rightTable + " order by c1"
        );

        assertThat(result.size()).isEqualTo(2);
        assertThat(((Number) result.value(0, "C1")).intValue()).isEqualTo(1);
        assertThat(result.value(0, "C2")).isEqualTo("A");
        assertThat(((Number) result.value(1, "C1")).intValue()).isEqualTo(3);
        assertThat(result.value(1, "C2")).isEqualTo("C");
    }

    @Test
    @DisplayName("Additional negative case: set operators reject mismatched column counts")
    void setOperatorRejectsMismatchedColumnCounts() {
        assertThatThrownBy(() ->
                jdbc.query(connection, "select 1 from dual intersect select 1, 2 from dual"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: set operators reject ORDER BY columns outside the result projection")
    void setOperatorRejectsOrderByColumnsOutsideProjection() {
        assertThatThrownBy(() ->
                jdbc.query(connection, "select 1 as c1 from dual intersect select 1 as c1 from dual order by c2"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional manual case: ROW_NUMBER and window frames work within partitions")
    void rowNumberAndWindowFrameWorkWithinPartitions() {
        String tableName = createWindowFixture("QA_WIN_FRAME");

        QueryResult result = jdbc.query(
                connection,
                "select grp, seq, amount, " +
                        "row_number() over(partition by grp order by seq) as rn, " +
                        "sum(amount) over(partition by grp order by seq rows between unbounded preceding and current row) as running_sum " +
                        "from " + tableName + " order by grp, seq"
        );

        assertThat(result.size()).isEqualTo(5);
        assertThat(((Number) result.value(0, "RN")).intValue()).isEqualTo(1);
        assertThat(((Number) result.value(1, "RN")).intValue()).isEqualTo(2);
        assertThat(((Number) result.value(2, "RN")).intValue()).isEqualTo(3);
        assertThat(((Number) result.value(0, "RUNNING_SUM")).intValue()).isEqualTo(10);
        assertThat(((Number) result.value(1, "RUNNING_SUM")).intValue()).isEqualTo(30);
        assertThat(((Number) result.value(2, "RUNNING_SUM")).intValue()).isEqualTo(60);
        assertThat(((Number) result.value(3, "RUNNING_SUM")).intValue()).isEqualTo(5);
        assertThat(((Number) result.value(4, "RUNNING_SUM")).intValue()).isEqualTo(20);
    }

    @Test
    @DisplayName("Additional manual case: NTILE and FIRST_VALUE/LAST_VALUE return partition-relative values")
    void ntileFirstValueAndLastValueWork() {
        String tableName = createWindowFixture("QA_WIN_VALUE");

        QueryResult result = jdbc.query(
                connection,
                "select seq, amount, " +
                        "first_value(amount) over(order by amount rows between unbounded preceding and unbounded following) as first_amount, " +
                        "last_value(amount) over(order by amount rows between unbounded preceding and unbounded following) as last_amount, " +
                        "ntile(2) over(order by amount) as bucket " +
                        "from " + tableName + " order by amount"
        );

        assertThat(result.size()).isEqualTo(5);
        assertThat(((Number) result.value(0, "FIRST_AMOUNT")).intValue()).isEqualTo(5);
        assertThat(((Number) result.value(4, "LAST_AMOUNT")).intValue()).isEqualTo(30);
        assertThat(((Number) result.value(0, "BUCKET")).intValue()).isEqualTo(1);
        assertThat(((Number) result.value(4, "BUCKET")).intValue()).isEqualTo(2);
    }

    @Test
    @DisplayName("Additional negative case: invalid window syntax fails during query execution")
    void invalidWindowSyntaxFails() {
        assertThatThrownBy(() ->
                jdbc.query(connection, "select row_number() over(partition by) from dual"))
                .isInstanceOf(IllegalStateException.class);
    }

    private void createSetOperatorFixture(String leftTable, String rightTable) {
        jdbc.executeUpdate(connection, "create table " + leftTable + "(c1 integer, c2 varchar(20))");
        jdbc.executeUpdate(connection, "create table " + rightTable + "(c1 integer, c2 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + leftTable + " values(1, 'A')");
        jdbc.executeUpdate(connection, "insert into " + leftTable + " values(2, 'B')");
        jdbc.executeUpdate(connection, "insert into " + leftTable + " values(2, 'B')");
        jdbc.executeUpdate(connection, "insert into " + leftTable + " values(3, 'C')");
        jdbc.executeUpdate(connection, "insert into " + rightTable + " values(2, 'B')");
        jdbc.executeUpdate(connection, "insert into " + rightTable + " values(4, 'D')");
    }

    private String createWindowFixture(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(grp integer, seq integer, amount integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 1, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 2, 20)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 3, 30)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 1, 5)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 2, 15)");
        return tableName;
    }
}
