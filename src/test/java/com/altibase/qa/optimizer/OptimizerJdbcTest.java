package com.altibase.qa.optimizer;

import Altibase.jdbc.driver.AltibaseConnection;
import Altibase.jdbc.driver.AltibaseStatement;
import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class OptimizerJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_365_001 ORDERED hint controls join order in the explain plan")
    void tc365001OrderedHint() throws Exception {
        JoinSample sample = createJoinSample("QA_OPT_ORDERED");

        ExecutedQuery executed = executeWithExplain(
                "select /*+ ORDERED */ distinct o.eno, e.e_lastname, c.c_lastname " +
                        "from " + sample.employeesTable + " e, " + sample.customersTable + " c, " + sample.ordersTable + " o " +
                        "where e.eno = o.eno and o.cno = c.cno"
        );

        assertThat(executed.rowCount()).isEqualTo(4);
        assertThat(executed.plan()).contains(sample.employeesTable, sample.customersTable, sample.ordersTable);
        assertThat(positionOf(executed.plan(), sample.employeesTable))
                .isLessThan(positionOf(executed.plan(), sample.customersTable));
        assertThat(positionOf(executed.plan(), sample.customersTable))
                .isLessThan(positionOf(executed.plan(), sample.ordersTable));
    }

    @Test
    @DisplayName("TC_365_002 optimizer can choose a join order without ORDERED hint")
    void tc365002OptimizerJoinOrder() throws Exception {
        JoinSample sample = createJoinSample("QA_OPT_AUTO");

        ExecutedQuery executed = executeWithExplain(
                "select distinct o.eno, e.e_lastname, c.c_lastname " +
                        "from " + sample.employeesTable + " e, " + sample.customersTable + " c, " + sample.ordersTable + " o " +
                        "where e.eno = o.eno and o.cno = c.cno"
        );

        assertThat(executed.rowCount()).isEqualTo(4);
        assertThat(executed.plan()).contains("JOIN");
        assertThat(executed.plan()).contains(sample.employeesTable, sample.customersTable, sample.ordersTable);
    }

    @Test
    @DisplayName("TC_366_001 SQL Plan Cache reuse metrics can be queried")
    void tc366001QueryPlanCacheReuse() throws Exception {
        JoinSample sample = createJoinSample("QA_PLAN_CACHE_REUSE");
        String sql = "select count(*) from " + sample.ordersTable + " where eno = 1";

        jdbc.queryForString(connection, sql);
        jdbc.queryForString(connection, sql);

        assertThat(jdbc.query(connection, "select max_cache_size, cache_hit_count, cache_miss_count from v$sql_plan_cache").rows())
                .hasSize(1);
        Number maxCacheSize = (Number) jdbc.query(connection, "select max_cache_size from v$sql_plan_cache").value(0, "MAX_CACHE_SIZE");
        Number hitCount = (Number) jdbc.query(connection, "select cache_hit_count from v$sql_plan_cache").value(0, "CACHE_HIT_COUNT");
        Number missCount = (Number) jdbc.query(connection, "select cache_miss_count from v$sql_plan_cache").value(0, "CACHE_MISS_COUNT");
        assertThat(maxCacheSize.longValue()).isGreaterThanOrEqualTo(0L);
        assertThat(hitCount.longValue()).isGreaterThanOrEqualTo(0L);
        assertThat(missCount.longValue()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @DisplayName("TC_367_001 SQL text and child PCO cache metrics can be queried together")
    void tc367001QueryPlanCacheSqlTextAndPco() {
        String tableName = createOrderedResultSample("QA_PLAN_CACHE_PCO");
        String sql = "select count(*) from " + tableName + " where dno = 1001";

        jdbc.queryForString(connection, sql);
        jdbc.queryForString(connection, sql);

        String escapedTableName = tableName.replace("'", "''");
        assertThat(jdbc.query(connection,
                "select a.sql_text, a.child_pco_count, b.hit_count, b.rebuild_count " +
                        "from v$sql_plan_cache_sqltext a, v$sql_plan_cache_pco b " +
                        "where a.sql_text_id = b.sql_text_id and a.sql_text like '%" + escapedTableName + "%'").rows())
                .isNotEmpty();
        assertThat(jdbc.queryForString(connection,
                "select a.sql_text from v$sql_plan_cache_sqltext a, v$sql_plan_cache_pco b " +
                        "where a.sql_text_id = b.sql_text_id and a.sql_text like '%" + escapedTableName + "%'"))
                .contains(tableName);
        Number childPcoCount = (Number) jdbc.query(connection,
                "select a.child_pco_count from v$sql_plan_cache_sqltext a, v$sql_plan_cache_pco b " +
                        "where a.sql_text_id = b.sql_text_id and a.sql_text like '%" + escapedTableName + "%'")
                .value(0, "CHILD_PCO_COUNT");
        Number hitCount = (Number) jdbc.query(connection,
                "select b.hit_count from v$sql_plan_cache_sqltext a, v$sql_plan_cache_pco b " +
                        "where a.sql_text_id = b.sql_text_id and a.sql_text like '%" + escapedTableName + "%'")
                .value(0, "HIT_COUNT");
        Number rebuildCount = (Number) jdbc.query(connection,
                "select b.rebuild_count from v$sql_plan_cache_sqltext a, v$sql_plan_cache_pco b " +
                        "where a.sql_text_id = b.sql_text_id and a.sql_text like '%" + escapedTableName + "%'")
                .value(0, "REBUILD_COUNT");
        assertThat(childPcoCount.longValue()).isGreaterThanOrEqualTo(1L);
        assertThat(hitCount.longValue()).isGreaterThanOrEqualTo(0L);
        assertThat(rebuildCount.longValue()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @DisplayName("TC_368_001 RESULT_CACHE hint changes the explain plan")
    void tc368001ResultCache() throws Exception {
        String tableName = createOrderedResultSample("QA_RESULT_CACHE");

        ExecutedQuery executed = executeWithExplain(
                "select /*+ RESULT_CACHE */ e_lastname, eno from " + tableName + " order by dno, e_firstname limit 3"
        );

        assertThat(executed.rowCount()).isEqualTo(3);
        assertThat(executed.plan().toUpperCase()).contains("RESULT");
        assertThat(executed.plan().toUpperCase()).contains("CACHE");
    }

    @Test
    @DisplayName("TC_369_001 TOP_RESULT_CACHE hint changes the explain plan")
    void tc369001TopResultCache() throws Exception {
        String tableName = createOrderedResultSample("QA_TOP_RESULT_CACHE");

        ExecutedQuery executed = executeWithExplain(
                "select /*+ TOP_RESULT_CACHE */ e_lastname, eno from " + tableName + " order by dno, e_firstname limit 3"
        );

        assertThat(executed.rowCount()).isEqualTo(3);
        assertThat(executed.plan().toUpperCase()).contains("TOP");
        assertThat(executed.plan().toUpperCase()).contains("CACHE");
    }

    private JoinSample createJoinSample(String prefix) {
        String employeesTable = DbTestSupport.uniqueName(prefix + "_EMP");
        String customersTable = DbTestSupport.uniqueName(prefix + "_CUS");
        String ordersTable = DbTestSupport.uniqueName(prefix + "_ORD");

        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, ordersTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, customersTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, employeesTable));

        jdbc.executeUpdate(connection, "create table " + employeesTable + "(eno integer primary key, e_lastname varchar(20), e_firstname varchar(20))");
        jdbc.executeUpdate(connection, "create table " + customersTable + "(cno integer primary key, c_lastname varchar(20))");
        jdbc.executeUpdate(connection, "create table " + ordersTable + "(ono integer, eno integer, cno integer)");
        jdbc.executeUpdate(connection, "create index " + DbTestSupport.uniqueName("QA_OPT_ORD_ENO") + " on " + ordersTable + "(eno)");
        jdbc.executeUpdate(connection, "create index " + DbTestSupport.uniqueName("QA_OPT_ORD_CNO") + " on " + ordersTable + "(cno)");

        jdbc.executeUpdate(connection, "insert into " + employeesTable + " values(1, 'MOON', 'CHAN')");
        jdbc.executeUpdate(connection, "insert into " + employeesTable + " values(2, 'DAVENPORT', 'SUSAN')");
        jdbc.executeUpdate(connection, "insert into " + employeesTable + " values(3, 'KOBAIN', 'KEN')");

        jdbc.executeUpdate(connection, "insert into " + customersTable + " values(10, 'BLAKE')");
        jdbc.executeUpdate(connection, "insert into " + customersTable + " values(20, 'SMITH')");
        jdbc.executeUpdate(connection, "insert into " + customersTable + " values(30, 'JONES')");

        jdbc.executeUpdate(connection, "insert into " + ordersTable + " values(100, 1, 10)");
        jdbc.executeUpdate(connection, "insert into " + ordersTable + " values(101, 2, 20)");
        jdbc.executeUpdate(connection, "insert into " + ordersTable + " values(102, 3, 30)");
        jdbc.executeUpdate(connection, "insert into " + ordersTable + " values(103, 1, 20)");

        return new JoinSample(employeesTable, customersTable, ordersTable);
    }

    private String createOrderedResultSample(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(eno integer, e_lastname varchar(20), e_firstname varchar(20), dno integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'MOON', 'CHAN-SEUNG', 3002)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'DAVENPORT', 'SUSAN', 1001)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 'KOBAIN', 'KEN', 1001)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(4, 'LEE', 'MINA', 1003)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(5, 'PARK', 'JUN', 1002)");

        return tableName;
    }

    private ExecutedQuery executeWithExplain(String sql) throws SQLException {
        AltibaseConnection altibaseConnection = (AltibaseConnection) connection;
        altibaseConnection.setExplainPlan(AltibaseConnection.EXPLAIN_PLAN_ON);
        try (AltibaseStatement statement = (AltibaseStatement) connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            int rowCount = 0;
            while (resultSet.next()) {
                rowCount++;
            }
            return new ExecutedQuery(rowCount, statement.getExplainPlan());
        } finally {
            altibaseConnection.setExplainPlan(AltibaseConnection.EXPLAIN_PLAN_OFF);
        }
    }

    private int positionOf(String plan, String tableName) {
        return plan.toUpperCase().indexOf(tableName.toUpperCase());
    }

    private record JoinSample(String employeesTable, String customersTable, String ordersTable) {
    }

    private record ExecutedQuery(int rowCount, String plan) {
    }
}
