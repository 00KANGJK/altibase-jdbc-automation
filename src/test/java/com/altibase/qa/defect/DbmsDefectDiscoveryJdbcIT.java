package com.altibase.qa.defect;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import com.altibase.qa.support.DbTestSupport;
import com.altibase.qa.support.FeatureProbe;
import com.altibase.qa.support.SqlExceptionSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Date;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings({"SqlNoDataSourceInspection", "SqlSourceToSinkFlow"})
class DbmsDefectDiscoveryJdbcIT extends BaseDbTest {

    private static final int FUZZ_SEED = 20260508;

    @Test
    @DisplayName("DEFECT-OPT-001 INNER JOIN and EXISTS return the same parent row set")
    void innerJoinAndExistsReturnSameRows() {
        JoinFixture fixture = createJoinFixture("QA_DEF_OPT_JOIN");

        assertSameRows(
                "select distinct p.id from " + fixture.parentTable + " p, " + fixture.childTable + " c " +
                        "where p.id = c.parent_id",
                "select p.id from " + fixture.parentTable + " p " +
                        "where exists (select 1 from " + fixture.childTable + " c where c.parent_id = p.id)"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-002 NOT EXISTS and LEFT JOIN IS NULL agree on NULL-heavy data")
    void notExistsAndLeftJoinIsNullAgreeOnNullHeavyData() {
        JoinFixture fixture = createJoinFixture("QA_DEF_OPT_ANTI");

        assertSameRows(
                "select p.id from " + fixture.parentTable + " p " +
                        "where not exists (select 1 from " + fixture.childTable + " c where c.parent_id = p.id)",
                "select p.id from " + fixture.parentTable + " p left outer join " + fixture.childTable + " c " +
                        "on c.parent_id = p.id where c.parent_id is null"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-003 GROUP BY HAVING rewrite keeps aggregate row sets stable")
    void groupByHavingRewriteKeepsAggregateRowsStable() {
        String tableName = createAggregateFixture("QA_DEF_OPT_GRP");

        assertSameRows(
                "select grp, count(val) as cnt, sum(val) as total from " + tableName +
                        " group by grp having count(val) >= 2",
                "select grp, cnt, total from (" +
                        "select grp, count(val) as cnt, sum(val) as total from " + tableName + " group by grp" +
                        ") where cnt >= 2"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-004 Creating indexes does not change predicate results")
    void creatingIndexesDoesNotChangePredicateResults() {
        String tableName = createFuzzFixture("QA_DEF_OPT_IDX");
        String sql = "select id, c_num, c_text from " + tableName +
                " where c_num between 2 and 5 and c_text <> 'SKIP'";

        List<String> beforeIndex = canonicalRows(sql);
        jdbc.executeUpdate(connection, "create index " + DbTestSupport.uniqueName("QA_DEF_IDX_NUM") + " on " + tableName + "(c_num)");
        jdbc.executeUpdate(connection, "create index " + DbTestSupport.uniqueName("QA_DEF_IDX_TEXT") + " on " + tableName + "(c_text)");
        List<String> afterIndex = canonicalRows(sql);

        assertThat(afterIndex).isEqualTo(beforeIndex);
    }

    @Test
    @DisplayName("DEFECT-OPT-005 LIMIT top-N matches client-side slicing of full sorted results")
    void limitTopNMatchesClientSideSortedSlice() {
        String tableName = createFuzzFixture("QA_DEF_OPT_TOP");

        List<String> fullOrder = orderedRows("select id, c_num from " + tableName + " order by c_num desc, id");
        List<String> topThree = orderedRows("select id, c_num from " + tableName + " order by c_num desc, id limit 3");

        assertThat(topThree).containsExactlyElementsOf(fullOrder.subList(0, 3));
    }

    @Test
    @DisplayName("DEFECT-OPT-006 ALL subquery with numeric/string type mismatch keeps SQL semantics")
    void allSubqueryWithNumericStringTypeMismatchKeepsSqlSemantics() {
        TypeMismatchFixture fixture = createTypeMismatchFixture("QA_DEF_OPT_ALL");

        assertSameRows(
                "select id from " + fixture.numericTable + " n where n.c_num > all (select c_text from " + fixture.textTable + ")",
                "select id from " + fixture.numericTable + " n where not exists (" +
                        "select 1 from " + fixture.textTable + " t where not (n.c_num > to_number(t.c_text)))"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-007 ANY subquery with numeric/string type mismatch keeps SQL semantics")
    void anySubqueryWithNumericStringTypeMismatchKeepsSqlSemantics() {
        TypeMismatchFixture fixture = createTypeMismatchFixture("QA_DEF_OPT_ANY");

        assertSameRows(
                "select id from " + fixture.numericTable + " n where n.c_num = any (select c_text from " + fixture.textTable + ")",
                "select id from " + fixture.numericTable + " n where exists (" +
                        "select 1 from " + fixture.textTable + " t where n.c_num = to_number(t.c_text))"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-008 Scalar subquery grouping matches direct GROUP BY with COUNT DISTINCT")
    void scalarSubqueryGroupingMatchesDirectGroupByWithCountDistinct() {
        String tableName = createGroupByAliasFixture("QA_DEF_OPT_GBA");

        assertSameRows(
                "select grp, count(distinct val) as cnt from " + tableName + " group by grp",
                "select (select t.grp from dual) as grp, count(distinct t.val) as cnt " +
                        "from " + tableName + " t group by t.grp"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-009 OR predicate with indexes matches UNION rewrite")
    void orPredicateWithIndexesMatchesUnionRewrite() {
        String tableName = createFuzzFixture("QA_DEF_OPT_OR");
        jdbc.executeUpdate(connection, "create index " + DbTestSupport.uniqueName("QA_DEF_OR_NUM") + " on " + tableName + "(c_num)");
        jdbc.executeUpdate(connection, "create index " + DbTestSupport.uniqueName("QA_DEF_OR_FLAG") + " on " + tableName + "(flag)");

        assertSameRows(
                "select id from " + tableName + " where c_num in (2, 5) or flag = 'N'",
                "select id from " + tableName + " where c_num in (2, 5) " +
                "union select id from " + tableName + " where flag = 'N'"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-010 NOT IN preserves NULL-aware anti-join semantics")
    void notInPreservesNullAwareAntiJoinSemantics() {
        TypeMismatchFixture fixture = createNumericAntiJoinFixture("QA_DEF_OPT_NOTIN");

        assertThat(canonicalRows(
                "select id from " + fixture.numericTable + " where c_num not in (select c_text from " + fixture.textTable + ")"))
                .isEmpty();
        assertSameRows(
                "select id from " + fixture.numericTable +
                        " where c_num not in (select c_text from " + fixture.textTable + " where c_text is not null)",
                "select n.id from " + fixture.numericTable + " n where not exists (" +
                        "select 1 from " + fixture.textTable + " t " +
                        "where t.c_text is not null and n.c_num = to_number(t.c_text))"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-011 Empty ALL and ANY subqueries keep quantified comparison semantics")
    void emptyAllAndAnySubqueriesKeepQuantifiedComparisonSemantics() {
        String tableName = createFuzzFixture("QA_DEF_OPT_EMPTY_Q");

        assertSameRows(
                "select id from " + tableName +
                        " where c_num > all (select c_num from " + tableName + " where 1 = 0)",
                "select id from " + tableName + " where not exists (" +
                        "select 1 from " + tableName + " s where 1 = 0 and not (" + tableName + ".c_num > s.c_num))"
        );
        assertSameRows(
                "select id from " + tableName +
                        " where c_num = any (select c_num from " + tableName + " where 1 = 0)",
                "select id from " + tableName + " where exists (" +
                        "select 1 from " + tableName + " s where 1 = 0 and " + tableName + ".c_num = s.c_num)"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-012 Scalar subquery HAVING returns NULL rather than fabricated zero")
    void scalarSubqueryHavingReturnsNullRatherThanFabricatedZero() {
        String outerTable = DbTestSupport.uniqueName("QA_DEF_OPT_SCALAR_O");
        String innerTable = DbTestSupport.uniqueName("QA_DEF_OPT_SCALAR_I");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, innerTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, outerTable));

        jdbc.executeUpdate(connection, "create table " + outerTable + "(a integer)");
        jdbc.executeUpdate(connection, "create table " + innerTable + "(a integer)");
        for (int value = 1; value <= 4; value++) {
            jdbc.executeUpdate(connection, "insert into " + outerTable + " values(" + value + ")");
        }
        jdbc.executeUpdate(connection, "insert into " + innerTable + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + innerTable + " values(2)");

        assertThat(orderedRows(
                "select a, (select count(*) from " + innerTable + " i " +
                        "where i.a = o.a having count(*) > 0) as cnt " +
                        "from " + outerTable + " o order by a"))
                .containsExactly("1|1", "2|1", "3|<NULL>", "4|<NULL>");
    }

    @Test
    @DisplayName("DEFECT-OPT-013 Correlated MIN subquery keeps the outer reference predicate")
    void correlatedMinSubqueryKeepsOuterReferencePredicate() {
        String outerTable = DbTestSupport.uniqueName("QA_DEF_OPT_MIN_O");
        String innerTable = DbTestSupport.uniqueName("QA_DEF_OPT_MIN_I");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, innerTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, outerTable));

        jdbc.executeUpdate(connection, "create table " + outerTable + "(pk integer primary key)");
        jdbc.executeUpdate(connection, "create table " + innerTable + "(pk integer primary key)");
        jdbc.executeUpdate(connection, "insert into " + outerTable + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + outerTable + " values(2)");
        jdbc.executeUpdate(connection, "insert into " + innerTable + " values(2)");

        assertThat(orderedRows(
                "select pk, (select min(42) from " + innerTable + " i where i.pk = o.pk) as marker " +
                        "from " + outerTable + " o order by pk"))
                .containsExactly("1|<NULL>", "2|42");
    }

    @Test
    @DisplayName("DEFECT-OPT-014 Filtered LEFT JOIN anti-join matches NOT EXISTS")
    void filteredLeftJoinAntiJoinMatchesNotExists() {
        JoinFixture fixture = createJoinFixture("QA_DEF_OPT_FILTERED_ANTI");

        assertSameRows(
                "select p.id from " + fixture.parentTable + " p where not exists (" +
                        "select 1 from " + fixture.childTable + " c " +
                        "where c.parent_id = p.id and c.note in ('A', 'D'))",
                "select p.id from " + fixture.parentTable + " p left outer join " + fixture.childTable + " c " +
                        "on c.parent_id = p.id and c.note in ('A', 'D') where c.id is null"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-015 Derived-table predicate pushdown preserves NULL semantics")
    void derivedTablePredicatePushdownPreservesNullSemantics() {
        String tableName = createFuzzFixture("QA_DEF_OPT_PUSH");

        assertSameRows(
                "select id from (select id, c_num, flag from " + tableName +
                        " where c_num is not null) v where c_num >= 2 and flag <> 'N'",
                "select id from " + tableName + " where c_num is not null and c_num >= 2 and flag <> 'N'"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-016 De Morgan rewrite preserves three-valued logic filtering")
    void deMorganRewritePreservesThreeValuedLogicFiltering() {
        String tableName = createFuzzFixture("QA_DEF_OPT_DEMORGAN");

        assertSameRows(
                "select id from " + tableName + " where not (c_num < 3 or flag = 'N')",
                "select id from " + tableName + " where c_num >= 3 and flag <> 'N'"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-017 Grouped LEFT JOIN counts match correlated scalar counts")
    void groupedLeftJoinCountsMatchCorrelatedScalarCounts() {
        JoinFixture fixture = createJoinFixture("QA_DEF_OPT_GROUP_JOIN");

        assertSameRows(
                "select p.id, count(c.id) as child_count from " + fixture.parentTable + " p " +
                        "left outer join " + fixture.childTable + " c on c.parent_id = p.id group by p.id",
                "select p.id, (select count(*) from " + fixture.childTable + " c where c.parent_id = p.id) as child_count " +
                        "from " + fixture.parentTable + " p"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-018 IN subquery with LEFT JOIN preserves semijoin cardinality")
    void inSubqueryWithLeftJoinPreservesSemijoinCardinality() {
        JoinFixture fixture = createJoinFixture("QA_DEF_OPT_SEMI_LEFT");

        assertSameRows(
                "select p.id from " + fixture.parentTable + " p where p.id in (" +
                        "select c.parent_id from " + fixture.childTable + " c left outer join " +
                        fixture.parentTable + " pp on pp.id = c.parent_id)",
                "select p.id from " + fixture.parentTable + " p where exists (" +
                        "select 1 from " + fixture.childTable + " c left outer join " +
                        fixture.parentTable + " pp on pp.id = c.parent_id where c.parent_id = p.id)"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-019 Double-nested IN subqueries do not duplicate outer rows")
    void doubleNestedInSubqueriesDoNotDuplicateOuterRows() {
        String tableName = createFuzzFixture("QA_DEF_OPT_DOUBLE_IN");

        assertSameRows(
                "select id from " + tableName + " o where 1 in (" +
                        "select 1 from " + tableName + " m where m.flag = o.flag and 1 in (" +
                        "select 1 from " + tableName + " i where i.c_text = m.c_text))",
                "select id from " + tableName + " o where exists (" +
                        "select 1 from " + tableName + " m where m.flag = o.flag and exists (" +
                        "select 1 from " + tableName + " i where i.c_text = m.c_text))"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-020 Aggregate derived-table join matches correlated aggregate filter")
    void aggregateDerivedTableJoinMatchesCorrelatedAggregateFilter() {
        String tableName = createAggregateFixture("QA_DEF_OPT_AGG_JOIN");

        assertSameRows(
                "select t.grp from " + tableName + " t join (" +
                        "select grp, sum(val) as total from " + tableName + " group by grp) s " +
                        "on s.grp = t.grp where s.total >= 30 group by t.grp",
                "select t.grp from " + tableName + " t where (" +
                        "select sum(i.val) from " + tableName + " i where i.grp = t.grp) >= 30 group by t.grp"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-021 OUTER JOIN filtered anti-join matches NOT EXISTS")
    void outerJoinFilteredAntiJoinMatchesNotExists() {
        String parent = DbTestSupport.uniqueName("QA_DEF_OPT_OUTER_P");
        String child = DbTestSupport.uniqueName("QA_DEF_OPT_OUTER_C");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(id integer primary key)");
        jdbc.executeUpdate(connection,
                "create table " + child + "(id integer primary key, parent_id integer, flag char(1))");
        jdbc.executeUpdate(connection, "insert into " + parent + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + parent + " values(2)");
        jdbc.executeUpdate(connection, "insert into " + parent + " values(3)");
        jdbc.executeUpdate(connection, "insert into " + child + " values(10, 1, 'Y')");
        jdbc.executeUpdate(connection, "insert into " + child + " values(11, 1, 'N')");
        jdbc.executeUpdate(connection, "insert into " + child + " values(12, 2, 'N')");
        jdbc.executeUpdate(connection, "insert into " + child + " values(13, null, 'Y')");

        assertSameRows(
                "select p.id from " + parent + " p left outer join " + child + " c " +
                        "on c.parent_id = p.id and c.flag = 'Y' where c.id is null",
                "select p.id from " + parent + " p where not exists (" +
                        "select 1 from " + child + " c where c.parent_id = p.id and c.flag = 'Y')"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-022 COUNT DISTINCT CASE matches derived DISTINCT")
    void countDistinctCaseMatchesDerivedDistinct() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_OPT_CD_CASE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(grp integer, val integer, flag char(1))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10, 'Y')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10, 'Y')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 20, 'N')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, null, 'Y')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 30, 'Y')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 30, 'N')");

        assertSameRows(
                "select grp, count(distinct case when flag = 'Y' then val else null end) as cnt " +
                        "from " + tableName + " group by grp",
                "select grp, count(*) as cnt from (" +
                        "select distinct grp, val from " + tableName +
                        " where flag = 'Y' and val is not null) group by grp"
        );
    }

    @Test
    @DisplayName("DEFECT-OPT-023 Constant-false UNION branch does not evaluate invalid conversion")
    void constantFalseUnionBranchDoesNotEvaluateInvalidConversion() {
        String sql = "select 1 as c1 from dual " +
                "union all select to_number('NOT_A_NUMBER') as c1 from dual where 1 = 0 " +
                "order by c1";

        assertThatCode(() -> orderedRows(sql))
                .as("a UNION ALL branch filtered by WHERE 1 = 0 must not evaluate its invalid projection")
                .doesNotThrowAnyException();
        assertThat(orderedRows(sql))
                .containsExactly("1");
    }

    @Test
    @DisplayName("DEFECT-OPT-024 CASE expression does not evaluate unreachable invalid conversion")
    void caseExpressionDoesNotEvaluateUnreachableInvalidConversion() {
        String thenBranchSql = "select case when 1 = 1 then 7 else to_number('NOT_A_NUMBER') end from dual";
        String elseBranchSql = "select case when 1 = 0 then to_number('NOT_A_NUMBER') else 9 end from dual";

        assertThatCode(() -> jdbc.queryForString(connection, thenBranchSql))
                .as("CASE must not evaluate an unreachable ELSE expression")
                .doesNotThrowAnyException();
        assertThat(jdbc.queryForString(connection, thenBranchSql))
                .isEqualTo("7");
        assertThatCode(() -> jdbc.queryForString(connection, elseBranchSql))
                .as("CASE must not evaluate an unreachable THEN expression")
                .doesNotThrowAnyException();
        assertThat(jdbc.queryForString(connection, elseBranchSql))
                .isEqualTo("9");
    }

    @Test
    @DisplayName("DEFECT-OPT-025 Simple CASE does not evaluate unreachable invalid conversion")
    void simpleCaseDoesNotEvaluateUnreachableInvalidConversion() {
        String sql = "select case 1 when 1 then 7 else to_number('NOT_A_NUMBER') end from dual";

        assertThatCode(() -> jdbc.queryForString(connection, sql))
                .as("simple CASE must not evaluate an unmatched ELSE expression")
                .doesNotThrowAnyException();
        assertThat(jdbc.queryForString(connection, sql)).isEqualTo("7");
    }

    @Test
    @DisplayName("DEFECT-OPT-026 COALESCE does not evaluate later arguments after first non-null")
    void coalesceDoesNotEvaluateLaterArgumentsAfterFirstNonNull() {
        String sql = "select coalesce(7, to_number('NOT_A_NUMBER')) from dual";

        assertThatCode(() -> jdbc.queryForString(connection, sql))
                .as("COALESCE must not evaluate later arguments once the first non-null value is selected")
                .doesNotThrowAnyException();
        assertThat(jdbc.queryForString(connection, sql)).isEqualTo("7");
    }

    @Test
    @DisplayName("DEFECT-OPT-027 EXISTS does not evaluate unused select-list expressions")
    void existsDoesNotEvaluateUnusedSelectListExpressions() {
        String sql = "select 1 as c1 from dual where exists (" +
                "select to_number('NOT_A_NUMBER') from dual)";

        assertThatCode(() -> orderedRows(sql))
                .as("EXISTS only needs row existence and must not evaluate an unused invalid select-list expression")
                .doesNotThrowAnyException();
        assertThat(orderedRows(sql)).containsExactly("1");
    }

    @Test
    @DisplayName("DEFECT-OPT-028 Empty scalar subquery does not evaluate invalid projection")
    void emptyScalarSubqueryDoesNotEvaluateInvalidProjection() {
        String sql = "select (select to_number('NOT_A_NUMBER') from dual where 1 = 0) as c1 from dual";

        assertThatCode(() -> orderedRows(sql))
                .as("a scalar subquery returning no rows must not evaluate its invalid projection")
                .doesNotThrowAnyException();
        assertThat(orderedRows(sql)).containsExactly("<NULL>");
    }

    @Test
    @DisplayName("DEFECT-OPT-029 NOT EXISTS empty subquery does not evaluate invalid projection")
    void notExistsEmptySubqueryDoesNotEvaluateInvalidProjection() {
        String sql = "select 1 as c1 from dual where not exists (" +
                "select to_number('NOT_A_NUMBER') from dual where 1 = 0)";

        assertThatCode(() -> orderedRows(sql))
                .as("NOT EXISTS over an empty subquery must not evaluate its invalid projection")
                .doesNotThrowAnyException();
        assertThat(orderedRows(sql)).containsExactly("1");
    }

    @Test
    @DisplayName("DEFECT-WIN-001 DISTINCT over LAG arithmetic matches derived-table DISTINCT")
    void distinctOverLagArithmeticMatchesDerivedTableDistinct() {
        String tableName = createWindowEdgeFixture("QA_DEF_WIN_LAG");

        assertSameRows(
                "select distinct grp, val - lag(val) over(partition by grp order by seq) as delta from " + tableName,
                "select distinct grp, delta from (" +
                        "select grp, val - lag(val) over(partition by grp order by seq) as delta from " + tableName + ")"
        );
    }

    @Test
    @DisplayName("DEFECT-WIN-002 ORDER BY repeated window expression matches ORDER BY alias")
    void orderByRepeatedWindowExpressionMatchesAlias() {
        String tableName = createWindowEdgeFixture("QA_DEF_WIN_ORDER");
        String window = "sum(val) over(partition by grp order by seq rows between unbounded preceding and current row)";

        assertThat(orderedRows(
                "select id, " + window + " as running from " + tableName + " order by running, id"))
                .isEqualTo(orderedRows(
                        "select id, " + window + " as running from " + tableName +
                                " order by " + window + ", id"));
    }

    @Test
    @DisplayName("DEFECT-WIN-003 ROW_NUMBER over grouped aggregate keeps aggregate values aligned")
    void rowNumberOverGroupedAggregateKeepsAggregateValuesAligned() {
        String tableName = createWindowEdgeFixture("QA_DEF_WIN_GROUP");

        assertThat(orderedRows(
                "select grp, sum(val) as total, row_number() over(order by sum(val) desc, grp) as rn " +
                        "from " + tableName + " group by grp order by rn"))
                .containsExactly("A|100|1", "B|70|2", "C|70|3");
    }

    @Test
    @DisplayName("DEFECT-WIN-004 Filtering derived window results preserves running-sum rows")
    void filteringDerivedWindowResultsPreservesRunningSumRows() {
        String tableName = createWindowEdgeFixture("QA_DEF_WIN_FILTER");

        assertThat(orderedRows(
                "select id, running from (" +
                        "select id, grp, seq, sum(val) over(partition by grp order by seq " +
                        "rows between unbounded preceding and current row) as running from " + tableName +
                        ") where running >= 60 order by id"))
                .containsExactly("2|100", "4|70", "5|70", "6|70");
    }

    @Test
    @DisplayName("DEFECT-FUZZ-001 Deterministic metamorphic SQL pairs keep equivalent row sets")
    void deterministicMetamorphicSqlPairsKeepEquivalentRows() {
        String tableName = createFuzzFixture("QA_DEF_FUZZ_" + FUZZ_SEED);

        assertSameRows(
                "select id from " + tableName + " where c_num >= 2 and c_text <> 'SKIP'",
                "select id from " + tableName + " where c_text <> 'SKIP' and c_num >= 2"
        );
        assertSameRows(
                "select id from " + tableName + " where c_num + 0 = c_num and c_num < 5",
                "select id from " + tableName + " where c_num < 5"
        );
        assertSameRows(
                "select id from " + tableName + " where c_num in (select c_num from " + tableName + " where flag = 'Y')",
                "select t.id from " + tableName + " t where exists (" +
                        "select 1 from " + tableName + " s where s.flag = 'Y' and s.c_num = t.c_num)"
        );
    }

    @Test
    @DisplayName("DEFECT-TX-001 Duplicate-key failure does not poison the current transaction")
    void duplicateKeyFailureDoesNotPoisonTransaction() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_DUP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, note varchar(20))");
        jdbc.begin(connection);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'first')");
            assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'dup')"))
                    .isInstanceOf(IllegalStateException.class);
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'after-error')");
            jdbc.commit(connection);
        } finally {
            rollbackQuietly(connection);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("2");
        assertThat(jdbc.queryForString(connection, "select min(id) || ':' || max(id) from " + tableName)).isEqualTo("1:2");
    }

    @Test
    @DisplayName("DEFECT-TX-002 Failed batch can be rolled back as one transaction")
    void failedBatchCanBeRolledBackAsOneTransaction() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_BATCH");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            statement.addBatch("insert into " + tableName + " values(1)");
            statement.addBatch("insert into " + tableName + " values(1)");

            assertThatThrownBy(statement::executeBatch).isInstanceOf(SQLException.class);
            connection.rollback();
        } finally {
            rollbackQuietly(connection);
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
    }

    @Test
    @DisplayName("DEFECT-TX-003 Repeated savepoint rollback leaves only the committed prefix")
    void repeatedSavepointRollbackLeavesOnlyCommittedPrefix() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_SP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
        connection.setAutoCommit(false);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            for (int value = 2; value <= 5; value++) {
                var savepoint = connection.setSavepoint("SP_" + value);
                jdbc.executeUpdate(connection, "insert into " + tableName + " values(" + value + ")");
                connection.rollback(savepoint);
            }
            connection.commit();
        } finally {
            rollbackQuietly(connection);
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select id from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-TX-004 Lock conflict does not poison the blocked connection")
    void lockConflictDoesNotPoisonBlockedConnection() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_LOCK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection blocked = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "lock table " + tableName + " in exclusive mode");

            jdbc.begin(blocked);
            assertThatThrownBy(() -> jdbc.executeUpdate(blocked, "lock table " + tableName + " in exclusive mode nowait"))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(jdbc.queryForString(blocked, "select count(*) from dual")).isEqualTo("1");
        } finally {
            rollbackQuietly(blocked);
            rollbackQuietly(connection);
            jdbc.closeQuietly(blocked);
        }
    }

    @Test
    @DisplayName("DEFECT-TX-005 Failed DDL inside explicit transaction does not poison later DML")
    void failedDdlInsideExplicitTransactionDoesNotPoisonLaterDml() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_DDL_FAIL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, note varchar(20))");
        connection.setAutoCommit(false);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'before')");
            assertThatThrownBy(() -> jdbc.executeUpdate(connection, "alter table " + tableName + " add column (id integer)"))
                    .isInstanceOf(IllegalStateException.class);
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'after')");
            connection.commit();
        } finally {
            rollbackQuietly(connection);
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("2");
    }

    @Test
    @DisplayName("DEFECT-TX-006 Lock failure can be retried after the holder rolls back")
    void lockFailureCanBeRetriedAfterHolderRollsBack() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_LOCK_RETRY");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection waiter = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "lock table " + tableName + " in exclusive mode");

            jdbc.begin(waiter);
            assertThatThrownBy(() -> jdbc.executeUpdate(waiter, "lock table " + tableName + " in exclusive mode nowait"))
                    .isInstanceOf(IllegalStateException.class);

            jdbc.rollback(connection);

            jdbc.executeUpdate(waiter, "lock table " + tableName + " in exclusive mode nowait");
            jdbc.executeUpdate(waiter, "insert into " + tableName + " values(1)");
            jdbc.commit(waiter);
        } finally {
            rollbackQuietly(waiter);
            rollbackQuietly(connection);
            jdbc.closeQuietly(waiter);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-TX-007 Prepared SELECT fails cleanly after table is dropped by another session")
    void preparedSelectFailsCleanlyAfterTableDroppedByAnotherSession() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_DROP_PREP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection reader = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

            try (PreparedStatement preparedStatement = jdbc.prepare(reader, "select count(*) from " + tableName)) {
                assertThat(singleString(preparedStatement)).isEqualTo("1");

                jdbc.executeUpdate(connection, "drop table " + tableName + " cascade");

                assertThatThrownBy(() -> singleString(preparedStatement))
                        .isInstanceOf(SQLException.class);
                assertThat(jdbc.queryForString(reader, "select count(*) from dual")).isEqualTo("1");
            }
        } finally {
            jdbc.closeQuietly(reader);
        }
    }

    @Test
    @DisplayName("DEFECT-TX-008 Prepared INSERT honors dropped columns made by another session")
    void preparedInsertHonorsDroppedColumnsMadeByAnotherSession() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_ALTER_PREP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection writer = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, payload integer)");

            try (PreparedStatement preparedStatement = jdbc.prepare(writer, "insert into " + tableName + "(id, payload) values(?, ?)")) {
                preparedStatement.setInt(1, 1);
                preparedStatement.setInt(2, 10);
                assertThat(preparedStatement.executeUpdate()).isEqualTo(1);

                jdbc.executeUpdate(connection, "alter table " + tableName + " drop column payload");

                preparedStatement.setInt(1, 2);
                preparedStatement.setInt(2, 20);
                assertThatThrownBy(preparedStatement::executeUpdate)
                        .isInstanceOf(SQLException.class);
                assertThat(jdbc.queryForString(writer, "select count(*) from dual")).isEqualTo("1");
            }
        } finally {
            jdbc.closeQuietly(writer);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-TX-009 Rollback after failed batch releases locks for another session")
    void rollbackAfterFailedBatchReleasesLocksForAnotherSession() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_BATCH_LOCK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection contender = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.addBatch("insert into " + tableName + " values(1)");
                statement.addBatch("insert into " + tableName + " values(1)");
                assertThatThrownBy(statement::executeBatch).isInstanceOf(SQLException.class);
                connection.rollback();
            } finally {
                rollbackQuietly(connection);
                connection.setAutoCommit(true);
            }

            jdbc.executeUpdate(contender, "insert into " + tableName + " values(2)");
            assertThat(jdbc.queryForString(contender, "select count(*) from " + tableName)).isEqualTo("1");
        } finally {
            jdbc.closeQuietly(contender);
        }
    }

    @Test
    @DisplayName("DEFECT-TX-010 Concurrent uncommitted delete is invisible until commit")
    void concurrentUncommittedDeleteIsInvisibleUntilCommit() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_DELETE_VIS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        Connection observer = jdbc.open();
        try {
            jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");

            jdbc.begin(connection);
            jdbc.executeUpdate(connection, "delete from " + tableName + " where id = 1");

            assertThat(jdbc.queryForString(observer, "select count(*) from " + tableName)).isEqualTo("2");

            jdbc.commit(connection);

            assertThat(jdbc.queryForString(observer, "select count(*) from " + tableName)).isEqualTo("1");
        } finally {
            rollbackQuietly(connection);
            jdbc.closeQuietly(observer);
        }
    }

    @Test
    @DisplayName("DEFECT-TX-011 SERIALIZABLE conflicts honor query timeout and prevent write-skew")
    void serializablePreventsWriteSkewInvariantViolations() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TX_WRITE_SKEW");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection,
                "create table " + tableName + "(id integer primary key, slot_id integer, active_flag char(1))");

        Connection first = jdbc.open();
        Connection second = jdbc.open();
        boolean firstCommitted = false;
        boolean secondCommitted = false;
        TimedUpdateResult firstInsert = null;
        TimedUpdateResult secondInsert = null;
        try {
            first.setAutoCommit(false);
            second.setAutoCommit(false);
            first.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            second.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            assertThat(jdbc.queryForString(first,
                    "select count(*) from " + tableName + " where slot_id = 1 and active_flag = 'Y'"))
                    .isEqualTo("0");
            assertThat(jdbc.queryForString(second,
                    "select count(*) from " + tableName + " where slot_id = 1 and active_flag = 'Y'"))
                    .isEqualTo("0");

            firstInsert = executeUpdateOrConflict(first,
                    "insert into " + tableName + " values(1, 1, 'Y')");
            if (firstInsert.succeeded()) {
                secondInsert = executeUpdateOrConflict(second,
                        "insert into " + tableName + " values(2, 1, 'Y')");
            }

            firstCommitted = firstInsert.succeeded() && commitOrSerializationFailure(first);
            secondCommitted = secondInsert != null && secondInsert.succeeded() && commitOrSerializationFailure(second);
        } finally {
            rollbackQuietly(first);
            rollbackQuietly(second);
            jdbc.closeQuietly(first);
            jdbc.closeQuietly(second);
        }

        assertThat(firstInsert).isNotNull();
        assertThat(firstInsert.elapsedSeconds())
                .as("Statement.setQueryTimeout(3) should bound SERIALIZABLE predicate-lock waits")
                .isLessThan(30.0);
        if (secondInsert != null) {
            assertThat(secondInsert.elapsedSeconds())
                    .as("Statement.setQueryTimeout(3) should bound the second SERIALIZABLE predicate-lock wait")
                    .isLessThan(30.0);
        }
        assertThat(firstCommitted && secondCommitted)
                .as("SERIALIZABLE must not allow two transactions to commit the same predicate-based invariant")
                .isFalse();
        assertThat(Integer.parseInt(jdbc.queryForString(connection,
                "select count(*) from " + tableName + " where slot_id = 1 and active_flag = 'Y'")))
                .isLessThanOrEqualTo(1);
    }

    @Test
    @DisplayName("DEFECT-TX-012 Foreign-key violation can rollback to savepoint and continue")
    void foreignKeyViolationCanRollbackToSavepointAndContinue() throws Exception {
        String parent = DbTestSupport.uniqueName("QA_DEF_TX_FK_P");
        String child = DbTestSupport.uniqueName("QA_DEF_TX_FK_C");
        String fkName = DbTestSupport.uniqueName("QA_DEF_TX_FK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(id integer primary key)");
        jdbc.executeUpdate(connection, "create table " + child +
                "(id integer primary key, parent_id integer, constraint " + fkName +
                " foreign key(parent_id) references " + parent + "(id))");
        jdbc.executeUpdate(connection, "insert into " + parent + " values(1)");

        Connection target = jdbc.open();
        try {
            target.setAutoCommit(false);
            jdbc.executeUpdate(target, "insert into " + child + " values(1, 1)");
            Savepoint beforeInvalidInsert = target.setSavepoint();

            assertThatThrownBy(() -> jdbc.executeUpdate(target, "insert into " + child + " values(2, 999)"))
                    .isInstanceOf(IllegalStateException.class);

            target.rollback(beforeInvalidInsert);
            jdbc.executeUpdate(target, "insert into " + child + " values(3, 1)");
            target.commit();
        } finally {
            rollbackQuietly(target);
            jdbc.closeQuietly(target);
        }

        assertThat(orderedRows("select id, parent_id from " + child + " order by id"))
                .containsExactly("1|1", "3|1");
    }

    @Test
    @DisplayName("DEFECT-TX-013 Cascade delete can be rolled back with parent and children restored")
    void cascadeDeleteCanBeRolledBackWithParentAndChildrenRestored() {
        String parent = DbTestSupport.uniqueName("QA_DEF_TX_CASCADE_P");
        String child = DbTestSupport.uniqueName("QA_DEF_TX_CASCADE_C");
        String fkName = DbTestSupport.uniqueName("QA_DEF_TX_CASCADE_FK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(id integer primary key)");
        jdbc.executeUpdate(connection, "create table " + child +
                "(id integer primary key, parent_id integer, constraint " + fkName +
                " foreign key(parent_id) references " + parent + "(id) on delete cascade)");
        jdbc.executeUpdate(connection, "insert into " + parent + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + child + " values(10, 1)");
        jdbc.executeUpdate(connection, "insert into " + child + " values(11, 1)");

        try {
            connection.setAutoCommit(false);
            jdbc.executeUpdate(connection, "delete from " + parent + " where id = 1");
            assertThat(jdbc.queryForString(connection, "select count(*) from " + parent)).isEqualTo("0");
            assertThat(jdbc.queryForString(connection, "select count(*) from " + child)).isEqualTo("0");

            connection.rollback();
        } catch (SQLException e) {
            throw new IllegalStateException("Cascade rollback scenario failed", e);
        } finally {
            rollbackQuietly(connection);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + parent)).isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + child)).isEqualTo("2");
    }

    @Test
    @DisplayName("DEFECT-TYPE-001 Implicit numeric comparison matches explicit TO_NUMBER comparison")
    void implicitNumericComparisonMatchesExplicitToNumber() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TYPE_NUM");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, c_num numeric(10,2), c_text varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 1, '001')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 2, '+2')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 3.50, '3.50')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(4, 4, '5')");

        assertSameRows(
                "select id from " + tableName + " where c_num = c_text",
                "select id from " + tableName + " where c_num = to_number(c_text)"
        );
    }

    @Test
    @DisplayName("DEFECT-TYPE-002 Invalid numeric conversion does not poison the session")
    void invalidNumericConversionDoesNotPoisonSession() {
        assertThatThrownBy(() -> jdbc.queryForString(connection, "select to_number('1A') from dual"))
                .isInstanceOf(IllegalStateException.class)
                .satisfies(throwable -> assertThat((Object) SqlExceptionSupport.findSqlException(throwable)).isNotNull());

        assertThat(jdbc.queryForString(connection, "select count(*) from dual")).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-TYPE-003 CHAR trailing-space semantics stay stable across equality and grouping")
    void charTrailingSpaceSemanticsStayStable() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TYPE_CHAR");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, c1 char(5))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'A')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'A   ')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 'B')");

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName + " where c1 = 'A'")).isEqualTo("2");
        assertThat(jdbc.queryForString(connection, "select count(*) from (select c1 from " + tableName + " group by c1)"))
                .isEqualTo("2");
    }

    @Test
    @DisplayName("DEFECT-TYPE-004 Date ordering remains stable around leap day and month end")
    void dateOrderingRemainsStableAroundLeapDayAndMonthEnd() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TYPE_DATE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, c1 date)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, to_date('2024-02-28','YYYY-MM-DD'))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, to_date('2024-02-29','YYYY-MM-DD'))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, to_date('2024-03-01','YYYY-MM-DD'))");

        assertThat(orderedRows("select id from " + tableName + " order by c1"))
                .containsExactly("1", "2", "3");
        assertThat(jdbc.queryForString(connection,
                "select to_char(last_day(to_date('2024-02-10','YYYY-MM-DD')), 'YYYY-MM-DD') from dual"))
                .isEqualTo("2024-02-29");
    }

    @Test
    @DisplayName("DEFECT-TYPE-005 Numeric precision overflow fails without altering valid rows")
    void numericPrecisionOverflowFailsWithoutAlteringValidRows() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TYPE_NUM_OVER");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, amount numeric(5,2))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 999.99)");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 1000.00)"))
                .as("numeric(5,2) must reject a value outside its declared precision")
                .isInstanceOf(IllegalStateException.class);

        assertThat(orderedRows("select id, amount from " + tableName + " order by id"))
                .containsExactly("1|999.99");
    }

    @Test
    @DisplayName("DEFECT-TYPE-006 LIKE ESCAPE matches literal wildcard characters")
    void likeEscapeMatchesLiteralWildcardCharacters() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TYPE_LIKE_ESC");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, txt varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'A_1')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'AB1')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 'A%1')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(4, 'A11')");

        assertThat(orderedRows("select id from " + tableName + " where txt like 'A\\_%' escape '\\' order by id"))
                .containsExactly("1");
        assertThat(orderedRows("select id from " + tableName + " where txt like 'A\\%%' escape '\\' order by id"))
                .containsExactly("3");
    }

    @Test
    @DisplayName("DEFECT-TYPE-007 UNION DISTINCT treats NULL rows as duplicates")
    void unionDistinctTreatsNullRowsAsDuplicates() {
        assertThat(orderedRows(
                "select cast(null as integer) as c1 from dual " +
                        "union select cast(null as integer) as c1 from dual"))
                .containsExactly("<NULL>");
    }

    @Test
    @DisplayName("DEFECT-DBL-001 Remote SQL error does not poison the local session")
    void remoteSqlErrorDoesNotPoisonLocalSession() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DEF_DBL_ERR");
        String localTable = DbTestSupport.uniqueName("QA_DEF_DBL_LOCAL");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, localTable));

        createPrivateDatabaseLink(linkName);

        assertThatThrownBy(() -> jdbc.query(connection,
                "select * from remote_table(" + linkName + ", 'select * from QA_DEF_MISSING_REMOTE')"))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "create table " + localTable + "(id integer primary key)");
        jdbc.executeUpdate(connection, "insert into " + localTable + " values(1)");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + localTable)).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-DBL-002 Dropped link failure does not break a newly created link")
    void droppedLinkFailureDoesNotBreakNewLink() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String droppedLink = DbTestSupport.uniqueName("QA_DEF_DBL_DROP");
        String workingLink = DbTestSupport.uniqueName("QA_DEF_DBL_NEW");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, droppedLink));
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, workingLink));

        createPrivateDatabaseLink(droppedLink);
        jdbc.executeUpdate(connection, "drop private database link " + droppedLink);

        assertThatThrownBy(() -> jdbc.query(connection,
                "select * from remote_table(" + droppedLink + ", 'select 1 from dual')"))
                .isInstanceOf(IllegalStateException.class);

        createPrivateDatabaseLink(workingLink);
        assertThat(jdbc.queryForString(connection,
                "select c1 from remote_table(" + workingLink + ", 'select 1 as c1 from dual')"))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-DBL-003 Remote session does not see uncommitted local rows until commit")
    void remoteSessionDoesNotSeeUncommittedLocalRowsUntilCommit() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DEF_DBL_ISO");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DBL_ISO_T");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
        createPrivateDatabaseLink(linkName);

        jdbc.begin(connection);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

            assertThat(jdbc.queryForString(connection,
                    "select c1 from remote_table(" + linkName +
                            ", 'select count(*) as c1 from " + tableName + "')"))
                    .isEqualTo("0");

            jdbc.commit(connection);
        } finally {
            rollbackQuietly(connection);
        }

        assertThat(jdbc.queryForString(connection,
                "select c1 from remote_table(" + linkName +
                        ", 'select count(*) as c1 from " + tableName + "')"))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-DBL-004 Remote execute error does not poison the local transaction")
    void remoteExecuteErrorDoesNotPoisonLocalTransaction() throws Exception {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DEF_DBL_TXERR");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DBL_TXERR_T");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createPrivateDatabaseLink(linkName);
        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");

        connection.setAutoCommit(false);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            assertThatThrownBy(() -> remoteExecuteImmediate(linkName, "insert into QA_DEF_DBL_MISSING values(1)"))
                    .isInstanceOf(IllegalStateException.class);
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
            connection.commit();
        } finally {
            rollbackQuietly(connection);
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("2");
    }

    @Test
    @DisplayName("DEFECT-DBL-005 Same-name database link recreate does not use stale dropped-link cache")
    void sameNameDatabaseLinkRecreateDoesNotUseStaleDroppedLinkCache() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DEF_DBL_REUSE");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));

        createPrivateDatabaseLink(linkName);
        assertThat(jdbc.queryForString(connection,
                "select c1 from remote_table(" + linkName + ", 'select 1 as c1 from dual')"))
                .isEqualTo("1");

        jdbc.executeUpdate(connection, "drop private database link " + linkName);
        assertThatThrownBy(() -> jdbc.query(connection,
                "select * from remote_table(" + linkName + ", 'select 1 from dual')"))
                .isInstanceOf(IllegalStateException.class);

        createPrivateDatabaseLink(linkName);
        assertThat(jdbc.queryForString(connection,
                "select c1 from remote_table(" + linkName + ", 'select 2 as c1 from dual')"))
                .isEqualTo("2");
    }

    @Test
    @DisplayName("DEFECT-DBL-006 Remote table type mapping matches the local projection")
    void remoteTableTypeMappingMatchesLocalProjection() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DEF_DBL_TYPES");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DBL_TYPES_T");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection,
                "create table " + tableName + "(" +
                        "id integer primary key, c_num numeric(10,2), c_text varchar(20), c_date date)");
        jdbc.executeUpdate(connection,
                "insert into " + tableName + " values(1, 10.50, 'ALPHA', to_date('2024-02-29','YYYY-MM-DD'))");
        jdbc.executeUpdate(connection,
                "insert into " + tableName + " values(2, null, null, null)");
        createPrivateDatabaseLink(linkName);

        assertSameRows(
                "select id, c_num, c_text, to_char(c_date, 'YYYY-MM-DD') as c_date_text from " + tableName,
                remoteTableSql(linkName,
                        "select id, c_num, c_text, to_char(c_date, 'YYYY-MM-DD') as c_date_text from " + tableName)
        );
    }

    @Test
    @DisplayName("DEFECT-DBL-007 Remote duplicate-key error does not poison the database-link session")
    void remoteDuplicateKeyErrorDoesNotPoisonDatabaseLinkSession() {
        FeatureProbe.assumeDatabaseLinkAvailable(config, jdbc, connection);

        String linkName = DbTestSupport.uniqueName("QA_DEF_DBL_DUP");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DBL_DUP_T");
        registerCleanup(() -> DbTestSupport.dropDatabaseLinkQuietly(jdbc, connection, linkName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createPrivateDatabaseLink(linkName);
        remoteExecuteImmediate(linkName, "create table " + tableName + "(id integer primary key)");
        remoteExecuteImmediate(linkName, "insert into " + tableName + " values(1)");

        assertThatThrownBy(() -> remoteExecuteImmediate(linkName, "insert into " + tableName + " values(1)"))
                .isInstanceOf(IllegalStateException.class);

        remoteExecuteImmediate(linkName, "insert into " + tableName + " values(2)");
        assertThat(jdbc.queryForString(connection,
                "select c1 from remote_table(" + linkName +
                        ", 'select count(*) as c1 from " + tableName + "')"))
                .isEqualTo("2");
    }

    @Test
    @DisplayName("DEFECT-SEC-001 Synonym cannot bypass revoked object SELECT privilege")
    void synonymCannotBypassRevokedObjectSelectPrivilege() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_OWNER");
        String consumer = createManagedUser("QA_DEF_SEC_CONS");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_TB");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_SEC_SYN");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1)");
        });
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + owner + "." + tableName);
            assertThat(jdbc.queryForString(conn, "select count(*) from " + synonymName)).isEqualTo("1");
        });

        jdbc.executeUpdate(connection, "revoke select on " + owner + "." + tableName + " from " + consumer);

        assertThatThrownBy(() -> withUserConnection(consumer, conn -> jdbc.queryForString(conn, "select count(*) from " + synonymName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("DEFECT-SEC-002 Revoked object SELECT privilege invalidates a prepared statement")
    void revokedObjectSelectPrivilegeInvalidatesPreparedStatement() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_PO");
        String consumer = createManagedUser("QA_DEF_SEC_PC");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_PREP");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1)");
        });
        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (PreparedStatement preparedStatement =
                         jdbc.prepare(conn, "select count(*) from " + owner + "." + tableName)) {
                assertThat(singleString(preparedStatement)).isEqualTo("1");

                jdbc.executeUpdate(connection, "revoke select on " + owner + "." + tableName + " from " + consumer);

                assertThatThrownBy(() -> singleString(preparedStatement))
                        .isInstanceOf(SQLException.class);
                assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-003 Private synonym shadows public synonym without leaking object access")
    void privateSynonymShadowsPublicSynonymWithoutLeakingObjectAccess() throws Exception {
        String publicOwner = createManagedUser("QA_DEF_SEC_PUBO");
        String privateOwner = createManagedUser("QA_DEF_SEC_PRIO");
        String consumer = createManagedUser("QA_DEF_SEC_SHADOW");
        String publicTable = DbTestSupport.uniqueName("QA_DEF_SEC_PUB_T");
        String privateTable = DbTestSupport.uniqueName("QA_DEF_SEC_PRI_T");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_SEC_SYN");
        registerCleanup(() -> DbTestSupport.dropPublicSynonymQuietly(jdbc, connection, synonymName));

        withUserConnection(publicOwner, conn -> {
            jdbc.executeUpdate(conn, "create table " + publicTable + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + publicTable + " values(100)");
        });
        withUserConnection(privateOwner, conn -> {
            jdbc.executeUpdate(conn, "create table " + privateTable + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + privateTable + " values(200)");
        });

        jdbc.executeUpdate(connection, "grant select on " + publicOwner + "." + publicTable + " to " + consumer);
        jdbc.executeUpdate(connection, "grant select on " + privateOwner + "." + privateTable + " to " + consumer);
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "create public synonym " + synonymName + " for " + publicOwner + "." + publicTable);

        withUserConnection(consumer, conn -> {
            assertThat(jdbc.queryForString(conn, "select id from " + synonymName)).isEqualTo("100");
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + privateOwner + "." + privateTable);
            assertThat(jdbc.queryForString(conn, "select id from " + synonymName)).isEqualTo("200");
            jdbc.executeUpdate(conn, "drop synonym " + synonymName);
            assertThat(jdbc.queryForString(conn, "select id from " + synonymName)).isEqualTo("100");
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-004 Revoked role removes cross-schema SELECT in a new session")
    void revokedRoleRemovesCrossSchemaSelectInNewSession() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_ROLE_O");
        String consumer = createManagedUser("QA_DEF_SEC_ROLE_C");
        String role = createManagedRole("QA_DEF_SEC_ROLE");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_ROLE_T");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1)");
        });
        jdbc.executeUpdate(connection, "grant select any table to " + role);
        jdbc.executeUpdate(connection, "grant " + role + " to " + consumer);

        withUserConnection(consumer, conn ->
                assertThat(jdbc.queryForString(conn, "select id from " + owner + "." + tableName)).isEqualTo("1"));

        jdbc.executeUpdate(connection, "revoke " + role + " from " + consumer);

        assertThatThrownBy(() -> withUserConnection(consumer, conn ->
                jdbc.queryForString(conn, "select id from " + owner + "." + tableName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("DEFECT-SEC-005 Synonym created with role-granted SELECT cannot bypass role revoke")
    void synonymCreatedWithRoleGrantedSelectCannotBypassRoleRevoke() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_SYNR_O");
        String consumer = createManagedUser("QA_DEF_SEC_SYNR_C");
        String role = createManagedRole("QA_DEF_SEC_SYNR_R");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_SYNR_T");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_SEC_SYNR");
        registerCleanup(() -> DbTestSupport.dropSynonymQuietly(jdbc, connection, consumer + "." + synonymName));

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(7)");
        });
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant select any table to " + role);
        jdbc.executeUpdate(connection, "grant " + role + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + owner + "." + tableName);
            assertThat(jdbc.queryForString(conn, "select id from " + synonymName)).isEqualTo("7");
        });

        jdbc.executeUpdate(connection, "revoke " + role + " from " + consumer);

        assertThatThrownBy(() -> withUserConnection(consumer, conn ->
                jdbc.queryForString(conn, "select id from " + synonymName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("DEFECT-SEC-006 Revoked object SELECT invalidates a dependent view")
    void revokedObjectSelectInvalidatesDependentView() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_VIEW_O");
        String consumer = createManagedUser("QA_DEF_SEC_VIEW_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_VIEW_T");
        String viewName = DbTestSupport.uniqueName("QA_DEF_SEC_VIEW");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, consumer + "." + viewName));

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(11)");
        });
        grantIfNeeded("create view", consumer);
        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create view " + viewName + " as select id from " + owner + "." + tableName);
            assertThat(jdbc.queryForString(conn, "select id from " + viewName)).isEqualTo("11");
        });

        jdbc.executeUpdate(connection, "revoke select on " + owner + "." + tableName + " from " + consumer);

        assertThatThrownBy(() -> withUserConnection(consumer, conn ->
                jdbc.queryForString(conn, "select id from " + viewName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("DEFECT-SEC-007 Revoked EXECUTE privilege invalidates a cached callable statement")
    void revokedExecutePrivilegeInvalidatesCachedCallableStatement() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_EXEC_O");
        String consumer = createManagedUser("QA_DEF_SEC_EXEC_C");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_SEC_EXEC_P");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (CallableStatement callableStatement = conn.prepareCall("{call " + owner + "." + procedureName + "(?)}")) {
                callableStatement.registerOutParameter(1, Types.INTEGER);
                callableStatement.execute();
                assertThat(callableStatement.getInt(1)).isEqualTo(1);

                jdbc.executeUpdate(connection, "revoke execute on " + owner + "." + procedureName + " from " + consumer);

                assertThatThrownBy(callableStatement::execute)
                        .isInstanceOf(SQLException.class);
                assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-008 Revoked INSERT privilege invalidates a cached prepared statement")
    void revokedInsertPrivilegeInvalidatesCachedPreparedStatement() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_INS_O");
        String consumer = createManagedUser("QA_DEF_SEC_INS_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_INS_T");

        withUserConnection(owner, conn -> jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)"));
        jdbc.executeUpdate(connection, "grant insert on " + owner + "." + tableName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (PreparedStatement preparedStatement =
                         jdbc.prepare(conn, "insert into " + owner + "." + tableName + " values(?)")) {
                preparedStatement.setInt(1, 1);
                assertThat(preparedStatement.executeUpdate()).isEqualTo(1);

                jdbc.executeUpdate(connection, "revoke insert on " + owner + "." + tableName + " from " + consumer);

                preparedStatement.setInt(1, 2);
                assertThatThrownBy(preparedStatement::executeUpdate)
                        .isInstanceOf(SQLException.class);
                assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
            }
        });

        assertThat(jdbc.queryForString(connection, "select count(*) from " + owner + "." + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-SEC-009 Revoked EXECUTE privilege blocks fresh callable statements")
    void revokedExecutePrivilegeBlocksFreshCallableStatements() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_EXF_O");
        String consumer = createManagedUser("QA_DEF_SEC_EXF_C");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_SEC_EXF_P");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn ->
                assertThat(executeOutProcedure(conn, owner + "." + procedureName)).isEqualTo(1));

        jdbc.executeUpdate(connection, "revoke execute on " + owner + "." + procedureName + " from " + consumer);

        assertThatThrownBy(() -> withUserConnection(consumer, conn ->
                executeOutProcedure(conn, owner + "." + procedureName)))
                .isInstanceOf(SQLException.class);
    }

    @Test
    @DisplayName("DEFECT-SEC-010 Revoking grant-option owner privilege removes downstream SELECT")
    void revokingGrantOptionOwnerPrivilegeRemovesDownstreamSelect() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_GO_O");
        String grantor = createManagedUser("QA_DEF_SEC_GO_G");
        String leaf = createManagedUser("QA_DEF_SEC_GO_L");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_GO_T");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(31)");
        });

        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + grantor + " with grant option");
        withUserConnection(grantor, conn -> jdbc.executeUpdate(conn,
                "grant select on " + owner + "." + tableName + " to " + leaf));
        withUserConnection(leaf, conn ->
                assertThat(jdbc.queryForString(conn, "select id from " + owner + "." + tableName)).isEqualTo("31"));

        jdbc.executeUpdate(connection, "revoke select on " + owner + "." + tableName + " from " + grantor);

        assertThatThrownBy(() -> withUserConnection(leaf, conn ->
                jdbc.queryForString(conn, "select id from " + owner + "." + tableName)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("DEFECT-SEC-011 Revoked EXECUTE privilege blocks fresh callable statements in the same session")
    void revokedExecutePrivilegeBlocksFreshCallableStatementsInSameSession() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_EXS_O");
        String consumer = createManagedUser("QA_DEF_SEC_EXS_C");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_SEC_EXS_P");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            assertThat(executeOutProcedure(conn, owner + "." + procedureName)).isEqualTo(1);

            jdbc.executeUpdate(connection, "revoke execute on " + owner + "." + procedureName + " from " + consumer);

            assertThatThrownBy(() -> executeOutProcedure(conn, owner + "." + procedureName))
                    .isInstanceOf(SQLException.class);
            assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-012 Revoked EXECUTE privilege invalidates a cached callable through a private synonym")
    void revokedExecutePrivilegeInvalidatesCachedCallableThroughPrivateSynonym() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_SYNP_O");
        String consumer = createManagedUser("QA_DEF_SEC_SYNP_C");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_SEC_SYNP_P");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_SEC_SYNP");
        registerCleanup(() -> DbTestSupport.dropSynonymQuietly(jdbc, connection, consumer + "." + synonymName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + owner + "." + procedureName);
            try (CallableStatement callableStatement = conn.prepareCall("{call " + synonymName + "(?)}")) {
                callableStatement.registerOutParameter(1, Types.INTEGER);
                callableStatement.execute();
                assertThat(callableStatement.getInt(1)).isEqualTo(1);

                jdbc.executeUpdate(connection, "revoke execute on " + owner + "." + procedureName + " from " + consumer);

                assertThatThrownBy(callableStatement::execute)
                        .isInstanceOf(SQLException.class);
                assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-013 Revoked EXECUTE privilege blocks fresh callable statements through a private synonym")
    void revokedExecutePrivilegeBlocksFreshCallableThroughPrivateSynonym() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_SYNF_O");
        String consumer = createManagedUser("QA_DEF_SEC_SYNF_C");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_SEC_SYNF_P");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_SEC_SYNF");
        registerCleanup(() -> DbTestSupport.dropSynonymQuietly(jdbc, connection, consumer + "." + synonymName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + owner + "." + procedureName);
            assertThat(executeOutProcedure(conn, synonymName)).isEqualTo(1);

            jdbc.executeUpdate(connection, "revoke execute on " + owner + "." + procedureName + " from " + consumer);

            assertThatThrownBy(() -> executeOutProcedure(conn, synonymName))
                    .isInstanceOf(SQLException.class);
            assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-014 Revoked EXECUTE privilege invalidates a cached function callable")
    void revokedExecutePrivilegeInvalidatesCachedFunctionCallable() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_FUNC_O");
        String consumer = createManagedUser("QA_DEF_SEC_FUNC_C");
        String functionName = DbTestSupport.uniqueName("QA_DEF_SEC_FUNC_F");
        registerCleanup(() -> DbTestSupport.dropFunctionQuietly(jdbc, connection, owner + "." + functionName));

        withUserConnection(owner, conn -> createSimpleFunction(conn, functionName));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + functionName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (CallableStatement callableStatement = conn.prepareCall("{ ? = call " + owner + "." + functionName + "(?) }")) {
                callableStatement.registerOutParameter(1, Types.INTEGER);
                callableStatement.setInt(2, 21);
                callableStatement.execute();
                assertThat(callableStatement.getInt(1)).isEqualTo(42);

                jdbc.executeUpdate(connection, "revoke execute on " + owner + "." + functionName + " from " + consumer);

                callableStatement.setInt(2, 22);
                assertThatThrownBy(callableStatement::execute)
                        .isInstanceOf(SQLException.class);
                assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-015 Revoked EXECUTE privilege blocks fresh function callables in the same session")
    void revokedExecutePrivilegeBlocksFreshFunctionCallablesInSameSession() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_FF_O");
        String consumer = createManagedUser("QA_DEF_SEC_FF_C");
        String functionName = DbTestSupport.uniqueName("QA_DEF_SEC_FF_F");
        registerCleanup(() -> DbTestSupport.dropFunctionQuietly(jdbc, connection, owner + "." + functionName));

        withUserConnection(owner, conn -> createSimpleFunction(conn, functionName));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + functionName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            assertThat(executeFunction(conn, owner + "." + functionName, 21)).isEqualTo(42);

            jdbc.executeUpdate(connection, "revoke execute on " + owner + "." + functionName + " from " + consumer);

            assertThatThrownBy(() -> executeFunction(conn, owner + "." + functionName, 22))
                    .isInstanceOf(SQLException.class);
            assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-016 Revoked role-granted EXECUTE invalidates a cached callable statement")
    void revokedRoleGrantedExecuteInvalidatesCachedCallableStatement() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_ROLEX_O");
        String consumer = createManagedUser("QA_DEF_SEC_ROLEX_C");
        String role = createManagedRole("QA_DEF_SEC_ROLEX_R");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_SEC_ROLEX_P");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> createSimpleOutProcedure(conn, procedureName));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + role);
        jdbc.executeUpdate(connection, "grant " + role + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (CallableStatement callableStatement = conn.prepareCall("{call " + owner + "." + procedureName + "(?)}")) {
                callableStatement.registerOutParameter(1, Types.INTEGER);
                callableStatement.execute();
                assertThat(callableStatement.getInt(1)).isEqualTo(1);

                jdbc.executeUpdate(connection, "revoke execute on " + owner + "." + procedureName + " from " + role);

                assertThatThrownBy(callableStatement::execute)
                        .isInstanceOf(SQLException.class);
                assertThat(jdbc.queryForString(conn, "select count(*) from dual")).isEqualTo("1");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-SEC-017 Definer procedure static SQL resolves unqualified names in owner schema")
    void definerProcedureStaticSqlResolvesUnqualifiedNamesInOwnerSchema() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_DEF_O");
        String consumer = createManagedUser("QA_DEF_SEC_DEF_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_DEF_T");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_SEC_DEF_P");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(val integer)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(777)");
            jdbc.executeUpdate(conn,
                    "create or replace procedure " + procedureName + "(p1 out integer) as begin " +
                            "select val into p1 from " + tableName + "; end;");
        });
        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(val integer)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1)");
        });
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn ->
                assertThat(executeOutProcedure(conn, owner + "." + procedureName)).isEqualTo(777));
    }

    @Test
    @DisplayName("DEFECT-SEC-018 Definer procedure dynamic SQL resolves unqualified DML in owner schema")
    void definerProcedureDynamicSqlResolvesUnqualifiedDmlInOwnerSchema() throws Exception {
        String owner = createManagedUser("QA_DEF_SEC_DYN_O");
        String consumer = createManagedUser("QA_DEF_SEC_DYN_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_SEC_DYN_T");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_SEC_DYN_P");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(val integer)");
            jdbc.executeUpdate(conn,
                    "create or replace procedure " + procedureName + " as begin " +
                            "execute immediate 'insert into " + tableName + " values(777)'; end;");
        });
        withUserConnection(consumer, conn ->
                jdbc.executeUpdate(conn, "create table " + tableName + "(val integer)"));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn -> executeNoArgProcedure(conn, owner + "." + procedureName));

        assertThat(jdbc.queryForString(connection, "select count(*) from " + owner + "." + tableName + " where val = 777"))
                .as("dynamic SQL inside a definer procedure must target the definer schema object")
                .isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + consumer + "." + tableName + " where val = 777"))
                .as("caller-owned same-name objects must not hijack definer procedure dynamic SQL")
                .isEqualTo("0");
    }

    @Test
    @DisplayName("DEFECT-META-001 Table statistics do not expose hidden row counts without SELECT")
    void tableStatisticsDoNotExposeHiddenRowCountsWithoutSelect() throws Exception {
        String owner = createManagedUser("QA_DEF_META_O");
        String consumer = createManagedUser("QA_DEF_META_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_META_T");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key, secret_value varchar(40))");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(1, 'SECRET_ALPHA')");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(2, 'SECRET_BETA')");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(3, 'SECRET_GAMMA')");
        });
        gatherTableStats(connection, owner, tableName);

        withUserConnection(consumer, conn -> {
            assertThatThrownBy(() -> jdbc.queryForString(conn, "select count(*) from " + owner + "." + tableName))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> getTableStats(conn, owner, tableName))
                    .isInstanceOf(IllegalStateException.class);
        });
    }

    @Test
    @DisplayName("DEFECT-META-002 JDBC metadata does not expose unauthorized table structure")
    void jdbcMetadataDoesNotExposeUnauthorizedTableStructure() throws Exception {
        String owner = createManagedUser("QA_DEF_META_STRUCT_O");
        String consumer = createManagedUser("QA_DEF_META_STRUCT_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_META_STRUCT_T");

        withUserConnection(owner, conn ->
                jdbc.executeUpdate(conn, "create table " + tableName +
                        "(id integer primary key, secret_token varchar(40), secret_amount integer)"));

        withUserConnection(consumer, conn -> {
            assertThatThrownBy(() -> jdbc.queryForString(conn, "select count(*) from " + owner + "." + tableName))
                    .isInstanceOf(IllegalStateException.class);
            boolean tableVisible = tableVisibleInMetadata(conn, owner, tableName);
            boolean columnVisible = columnVisibleInMetadata(conn, owner, tableName, "SECRET_TOKEN");

            assertThat(List.of(tableVisible, columnVisible))
                    .as("JDBC metadata visibility without object privileges [tableVisible, columnVisible]")
                    .containsExactly(false, false);
        });
    }

    @Test
    @DisplayName("DEFECT-DDL-001 Same-name table recreate invalidates cached prepared statements")
    void sameNameTableRecreateInvalidatesCachedPreparedStatement() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_DDL_TABLE_RECREATE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10)");

        try (PreparedStatement preparedStatement = connection.prepareStatement(
                "select val from " + tableName + " where id = ?")) {
            preparedStatement.setInt(1, 1);
            assertThat(singleString(preparedStatement)).isEqualTo("10");

            jdbc.executeUpdate(connection, "drop table " + tableName + " cascade");
            jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 20)");

            preparedStatement.setInt(1, 1);
            assertCachedPreparedStatementYieldsFreshOrFails(preparedStatement, "20");
        }

        assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 1"))
                .isEqualTo("20");
    }

    @Test
    @DisplayName("DEFECT-DDL-002 Same-name procedure recreate invalidates cached callables")
    void sameNameProcedureRecreateInvalidatesCachedCallables() throws Exception {
        String owner = createManagedUser("QA_DEF_DDL_PROC_O");
        String consumer = createManagedUser("QA_DEF_DDL_PROC_C");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_DDL_PROC_P");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, owner + "." + procedureName));

        withUserConnection(owner, conn -> createOutProcedureReturning(conn, procedureName, 1));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (CallableStatement callableStatement = conn.prepareCall(
                    "{call " + owner + "." + procedureName + "(?)}")) {
                callableStatement.registerOutParameter(1, Types.INTEGER);
                callableStatement.execute();
                assertThat(callableStatement.getInt(1)).isEqualTo(1);

                jdbc.executeUpdate(connection, "drop procedure " + owner + "." + procedureName);
                withUserConnection(owner, ownerConn -> createOutProcedureReturning(ownerConn, procedureName, 2));
                jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

                assertThat(executeOutProcedure(conn, owner + "." + procedureName)).isEqualTo(2);
                assertCachedOutProcedureYieldsFreshOrFails(callableStatement, 2);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DDL-003 Same-name function recreate invalidates cached function callables")
    void sameNameFunctionRecreateInvalidatesCachedFunctionCallables() throws Exception {
        String owner = createManagedUser("QA_DEF_DDL_FUNC_O");
        String consumer = createManagedUser("QA_DEF_DDL_FUNC_C");
        String functionName = DbTestSupport.uniqueName("QA_DEF_DDL_FUNC_F");
        registerCleanup(() -> DbTestSupport.dropFunctionQuietly(jdbc, connection, owner + "." + functionName));

        withUserConnection(owner, conn -> createFunctionMultiplier(conn, functionName, 2));
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + functionName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (CallableStatement callableStatement = conn.prepareCall(
                    "{ ? = call " + owner + "." + functionName + "(?) }")) {
                callableStatement.registerOutParameter(1, Types.INTEGER);
                callableStatement.setInt(2, 21);
                callableStatement.execute();
                assertThat(callableStatement.getInt(1)).isEqualTo(42);

                jdbc.executeUpdate(connection, "drop function " + owner + "." + functionName);
                withUserConnection(owner, ownerConn -> createFunctionMultiplier(ownerConn, functionName, 3));
                jdbc.executeUpdate(connection, "grant execute on " + owner + "." + functionName + " to " + consumer);

                assertThat(executeFunction(conn, owner + "." + functionName, 21)).isEqualTo(63);
                assertCachedFunctionYieldsFreshOrFails(callableStatement, 21, 63);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DDL-004 Private synonym retarget invalidates cached prepared statements")
    void privateSynonymRetargetInvalidatesCachedPreparedStatements() throws Exception {
        String firstOwner = createManagedUser("QA_DEF_DDL_SYN_A");
        String secondOwner = createManagedUser("QA_DEF_DDL_SYN_B");
        String consumer = createManagedUser("QA_DEF_DDL_SYN_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DDL_SYN_T");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_DDL_SYN");

        withUserConnection(firstOwner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(10)");
        });
        withUserConnection(secondOwner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(20)");
        });
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant select on " + firstOwner + "." + tableName + " to " + consumer);
        jdbc.executeUpdate(connection, "grant select on " + secondOwner + "." + tableName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + firstOwner + "." + tableName);

            try (PreparedStatement preparedStatement = conn.prepareStatement(
                    "select id from " + synonymName)) {
                assertThat(singleString(preparedStatement)).isEqualTo("10");

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + secondOwner + "." + tableName);
                assertThat(jdbc.queryForString(conn, "select id from " + synonymName)).isEqualTo("20");

                assertCachedPreparedStatementYieldsFreshOrFails(preparedStatement, "20");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DDL-005 Private synonym drop invalidates cached prepared statements")
    void privateSynonymDropInvalidatesCachedPreparedStatements() throws Exception {
        String owner = createManagedUser("QA_DEF_DDL_SYND_O");
        String consumer = createManagedUser("QA_DEF_DDL_SYND_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DDL_SYND_T");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_DDL_SYND");

        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(10)");
        });
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + owner + "." + tableName);

            try (PreparedStatement preparedStatement = conn.prepareStatement("select id from " + synonymName)) {
                assertThat(singleString(preparedStatement)).isEqualTo("10");

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                assertThatThrownBy(() -> jdbc.queryForString(conn, "select id from " + synonymName))
                        .isInstanceOf(IllegalStateException.class);
                assertCachedPreparedStatementFails(preparedStatement);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DDL-006 Private synonym shadowing invalidates cached prepared statements")
    void privateSynonymShadowingInvalidatesCachedPreparedStatements() throws Exception {
        String publicOwner = createManagedUser("QA_DEF_DDL_SYNS_P");
        String privateOwner = createManagedUser("QA_DEF_DDL_SYNS_R");
        String consumer = createManagedUser("QA_DEF_DDL_SYNS_C");
        String publicTable = DbTestSupport.uniqueName("QA_DEF_DDL_SYNS_PT");
        String privateTable = DbTestSupport.uniqueName("QA_DEF_DDL_SYNS_RT");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_DDL_SYNS");
        registerCleanup(() -> DbTestSupport.dropPublicSynonymQuietly(jdbc, connection, synonymName));

        withUserConnection(publicOwner, conn -> {
            jdbc.executeUpdate(conn, "create table " + publicTable + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + publicTable + " values(100)");
        });
        withUserConnection(privateOwner, conn -> {
            jdbc.executeUpdate(conn, "create table " + privateTable + "(id integer primary key)");
            jdbc.executeUpdate(conn, "insert into " + privateTable + " values(200)");
        });
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant select on " + publicOwner + "." + publicTable + " to " + consumer);
        jdbc.executeUpdate(connection, "grant select on " + privateOwner + "." + privateTable + " to " + consumer);
        jdbc.executeUpdate(connection,
                "create public synonym " + synonymName + " for " + publicOwner + "." + publicTable);

        withUserConnection(consumer, conn -> {
            try (PreparedStatement preparedStatement = conn.prepareStatement("select id from " + synonymName)) {
                assertThat(singleString(preparedStatement)).isEqualTo("100");

                jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + privateOwner + "." + privateTable);
                assertThat(jdbc.queryForString(conn, "select id from " + synonymName)).isEqualTo("200");

                assertCachedPreparedStatementYieldsFreshOrFails(preparedStatement, "200");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DDL-007 View replacement invalidates cached prepared statements")
    void viewReplacementInvalidatesCachedPreparedStatements() throws Exception {
        String viewName = DbTestSupport.uniqueName("QA_DEF_DDL_VIEW");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, viewName));

        jdbc.executeUpdate(connection, "create or replace view " + viewName + " as select 10 as c1 from dual");

        try (PreparedStatement preparedStatement = connection.prepareStatement("select c1 from " + viewName)) {
            assertThat(singleString(preparedStatement)).isEqualTo("10");

            jdbc.executeUpdate(connection, "create or replace view " + viewName + " as select 20 as c1 from dual");
            assertThat(jdbc.queryForString(connection, "select c1 from " + viewName)).isEqualTo("20");

            assertCachedPreparedStatementYieldsFreshOrFails(preparedStatement, "20");
        }
    }

    @Test
    @DisplayName("DEFECT-DML-001 Synonym retarget invalidates cached INSERT targets")
    void synonymRetargetInvalidatesCachedInsertTargets() throws Exception {
        String firstOwner = createManagedUser("QA_DEF_DML_INS_A");
        String secondOwner = createManagedUser("QA_DEF_DML_INS_B");
        String consumer = createManagedUser("QA_DEF_DML_INS_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DML_INS_T");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_DML_INS_SYN");

        createDmlTarget(firstOwner, tableName, 10, 100);
        createDmlTarget(secondOwner, tableName, 20, 200);
        grantIfNeeded("create synonym", consumer);
        grantTablePrivileges(firstOwner, tableName, consumer, "select", "insert");
        grantTablePrivileges(secondOwner, tableName, consumer, "select", "insert");

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + firstOwner + "." + tableName);

            try (PreparedStatement cached = conn.prepareStatement(
                    "insert into " + synonymName + "(id, val) values(?, ?)")) {
                setInts(cached, 11, 101);
                cached.executeUpdate();
                assertThat(rowCount(connection, firstOwner + "." + tableName, "id = 11")).isEqualTo(1);

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + secondOwner + "." + tableName);
                jdbc.executeUpdate(conn, "insert into " + synonymName + "(id, val) values(21, 201)");
                assertThat(rowCount(connection, secondOwner + "." + tableName, "id = 21")).isEqualTo(1);

                setInts(cached, 12, 102);
                executeCachedUpdateOrAcceptFailure(cached);

                assertThat(rowCount(connection, firstOwner + "." + tableName, "id = 12"))
                        .as("cached INSERT must not write through the old synonym target")
                        .isEqualTo(0);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DML-002 Synonym retarget invalidates cached UPDATE targets")
    void synonymRetargetInvalidatesCachedUpdateTargets() throws Exception {
        String firstOwner = createManagedUser("QA_DEF_DML_UPD_A");
        String secondOwner = createManagedUser("QA_DEF_DML_UPD_B");
        String consumer = createManagedUser("QA_DEF_DML_UPD_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DML_UPD_T");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_DML_UPD_SYN");

        createDmlTarget(firstOwner, tableName, 1, 10);
        createDmlTarget(secondOwner, tableName, 1, 20);
        grantIfNeeded("create synonym", consumer);
        grantTablePrivileges(firstOwner, tableName, consumer, "select", "update");
        grantTablePrivileges(secondOwner, tableName, consumer, "select", "update");

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + firstOwner + "." + tableName);

            try (PreparedStatement cached = conn.prepareStatement(
                    "update " + synonymName + " set val = ? where id = ?")) {
                setInts(cached, 11, 1);
                cached.executeUpdate();
                assertThat(jdbc.queryForString(connection,
                        "select val from " + firstOwner + "." + tableName + " where id = 1")).isEqualTo("11");

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + secondOwner + "." + tableName);
                jdbc.executeUpdate(conn, "update " + synonymName + " set val = 21 where id = 1");
                assertThat(jdbc.queryForString(connection,
                        "select val from " + secondOwner + "." + tableName + " where id = 1")).isEqualTo("21");

                setInts(cached, 99, 1);
                executeCachedUpdateOrAcceptFailure(cached);

                assertThat(jdbc.queryForString(connection,
                        "select val from " + firstOwner + "." + tableName + " where id = 1"))
                        .as("cached UPDATE must not modify the old synonym target")
                        .isEqualTo("11");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DML-003 Synonym retarget invalidates cached DELETE targets")
    void synonymRetargetInvalidatesCachedDeleteTargets() throws Exception {
        String firstOwner = createManagedUser("QA_DEF_DML_DEL_A");
        String secondOwner = createManagedUser("QA_DEF_DML_DEL_B");
        String consumer = createManagedUser("QA_DEF_DML_DEL_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DML_DEL_T");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_DML_DEL_SYN");

        createDmlTarget(firstOwner, tableName, 1, 10);
        insertDmlRow(firstOwner, tableName, 3, 30);
        createDmlTarget(secondOwner, tableName, 1, 20);
        insertDmlRow(secondOwner, tableName, 3, 40);
        grantIfNeeded("create synonym", consumer);
        grantTablePrivileges(firstOwner, tableName, consumer, "select", "delete");
        grantTablePrivileges(secondOwner, tableName, consumer, "select", "delete");

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + firstOwner + "." + tableName);

            try (PreparedStatement cached = conn.prepareStatement(
                    "delete from " + synonymName + " where id = ?")) {
                cached.setInt(1, 3);
                cached.executeUpdate();
                assertThat(rowCount(connection, firstOwner + "." + tableName, "id = 3")).isEqualTo(0);

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + secondOwner + "." + tableName);
                jdbc.executeUpdate(conn, "delete from " + synonymName + " where id = 3");
                assertThat(rowCount(connection, secondOwner + "." + tableName, "id = 3")).isEqualTo(0);

                cached.setInt(1, 1);
                executeCachedUpdateOrAcceptFailure(cached);

                assertThat(rowCount(connection, firstOwner + "." + tableName, "id = 1"))
                        .as("cached DELETE must not delete from the old synonym target")
                        .isEqualTo(1);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DML-004 Dropped synonym invalidates cached INSERT targets")
    void droppedSynonymInvalidatesCachedInsertTargets() throws Exception {
        String owner = createManagedUser("QA_DEF_DML_DROP_O");
        String consumer = createManagedUser("QA_DEF_DML_DROP_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_DML_DROP_T");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_DML_DROP_SYN");

        createDmlTarget(owner, tableName, 1, 10);
        grantIfNeeded("create synonym", consumer);
        grantTablePrivileges(owner, tableName, consumer, "select", "insert");

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + owner + "." + tableName);

            try (PreparedStatement cached = conn.prepareStatement(
                    "insert into " + synonymName + "(id, val) values(?, ?)")) {
                setInts(cached, 2, 20);
                cached.executeUpdate();
                assertThat(rowCount(connection, owner + "." + tableName, "id = 2")).isEqualTo(1);

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                assertThatThrownBy(() ->
                        jdbc.executeUpdate(conn, "insert into " + synonymName + "(id, val) values(3, 30)"))
                        .isInstanceOf(IllegalStateException.class);

                setInts(cached, 4, 40);
                executeCachedUpdateOrAcceptFailure(cached);

                assertThat(rowCount(connection, owner + "." + tableName, "id = 4"))
                        .as("cached INSERT must not write after the synonym has been dropped")
                        .isEqualTo(0);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-DML-005 Private synonym shadowing invalidates cached INSERT targets")
    void privateSynonymShadowingInvalidatesCachedInsertTargets() throws Exception {
        String publicOwner = createManagedUser("QA_DEF_DML_SH_P");
        String privateOwner = createManagedUser("QA_DEF_DML_SH_R");
        String consumer = createManagedUser("QA_DEF_DML_SH_C");
        String publicTable = DbTestSupport.uniqueName("QA_DEF_DML_SH_PT");
        String privateTable = DbTestSupport.uniqueName("QA_DEF_DML_SH_RT");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_DML_SH_SYN");
        registerCleanup(() -> DbTestSupport.dropPublicSynonymQuietly(jdbc, connection, synonymName));

        createDmlTarget(publicOwner, publicTable, 1, 100);
        createDmlTarget(privateOwner, privateTable, 1, 200);
        grantIfNeeded("create synonym", consumer);
        grantTablePrivileges(publicOwner, publicTable, consumer, "select", "insert");
        grantTablePrivileges(privateOwner, privateTable, consumer, "select", "insert");
        jdbc.executeUpdate(connection,
                "create public synonym " + synonymName + " for " + publicOwner + "." + publicTable);

        withUserConnection(consumer, conn -> {
            try (PreparedStatement cached = conn.prepareStatement(
                    "insert into " + synonymName + "(id, val) values(?, ?)")) {
                setInts(cached, 2, 102);
                cached.executeUpdate();
                assertThat(rowCount(connection, publicOwner + "." + publicTable, "id = 2")).isEqualTo(1);

                jdbc.executeUpdate(conn,
                        "create synonym " + synonymName + " for " + privateOwner + "." + privateTable);
                jdbc.executeUpdate(conn, "insert into " + synonymName + "(id, val) values(2, 202)");
                assertThat(rowCount(connection, privateOwner + "." + privateTable, "id = 2")).isEqualTo(1);

                setInts(cached, 3, 103);
                executeCachedUpdateOrAcceptFailure(cached);

                assertThat(rowCount(connection, publicOwner + "." + publicTable, "id = 3"))
                        .as("cached INSERT must not use the shadowed public synonym target")
                        .isEqualTo(0);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-CONS-001 Added CHECK constraint invalidates cached INSERT statements")
    void addedCheckConstraintInvalidatesCachedInsertStatements() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_CONS_CK_ADD");
        String constraintName = DbTestSupport.uniqueName("QA_DEF_CK_ADD");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");

        try (PreparedStatement cached = connection.prepareStatement(
                "insert into " + tableName + "(id, val) values(?, ?)")) {
            setInts(cached, 1, 1);
            cached.executeUpdate();

            jdbc.executeUpdate(connection,
                    "alter table " + tableName + " add constraint " + constraintName + " check(val > 0)");
            assertThatThrownBy(() ->
                    jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(2, -1)"))
                    .isInstanceOf(IllegalStateException.class);

            setInts(cached, 3, -1);
            assertCachedPreparedUpdateFails(cached);
            assertThat(rowCount(connection, tableName, "id = 3")).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("DEFECT-CONS-002 Added unique index invalidates cached INSERT statements")
    void addedUniqueIndexInvalidatesCachedInsertStatements() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_CONS_UK_ADD");
        String indexName = DbTestSupport.uniqueName("QA_DEF_UK_ADD");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");

        try (PreparedStatement cached = connection.prepareStatement(
                "insert into " + tableName + "(id, val) values(?, ?)")) {
            setInts(cached, 1, 10);
            cached.executeUpdate();

            jdbc.executeUpdate(connection, "create unique index " + indexName + " on " + tableName + "(val)");
            assertThatThrownBy(() ->
                    jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(2, 10)"))
                    .isInstanceOf(IllegalStateException.class);

            setInts(cached, 3, 10);
            assertCachedPreparedUpdateFails(cached);
            assertThat(rowCount(connection, tableName, "id = 3")).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("DEFECT-CONS-003 Added foreign key invalidates cached INSERT statements")
    void addedForeignKeyInvalidatesCachedInsertStatements() throws Exception {
        String parent = DbTestSupport.uniqueName("QA_DEF_CONS_FK_P");
        String child = DbTestSupport.uniqueName("QA_DEF_CONS_FK_C");
        String fkName = DbTestSupport.uniqueName("QA_DEF_FK_ADD");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, child));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parent));

        jdbc.executeUpdate(connection, "create table " + parent + "(id integer primary key)");
        jdbc.executeUpdate(connection, "create table " + child + "(id integer primary key, parent_id integer)");
        jdbc.executeUpdate(connection, "insert into " + parent + " values(1)");

        try (PreparedStatement cached = connection.prepareStatement(
                "insert into " + child + "(id, parent_id) values(?, ?)")) {
            setInts(cached, 1, 1);
            cached.executeUpdate();

            jdbc.executeUpdate(connection,
                    "alter table " + child + " add constraint " + fkName +
                            " foreign key(parent_id) references " + parent + "(id)");
            assertThatThrownBy(() ->
                    jdbc.executeUpdate(connection, "insert into " + child + "(id, parent_id) values(2, 999)"))
                    .isInstanceOf(IllegalStateException.class);

            setInts(cached, 3, 999);
            assertCachedPreparedUpdateFails(cached);
            assertThat(rowCount(connection, child, "id = 3")).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("DEFECT-CONS-004 Dropped CHECK constraint invalidates cached INSERT statements")
    void droppedCheckConstraintInvalidatesCachedInsertStatements() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_CONS_CK_DROP");
        String constraintName = DbTestSupport.uniqueName("QA_DEF_CK_DROP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection,
                "create table " + tableName + "(id integer primary key, val integer constraint " +
                        constraintName + " check(val > 0))");

        try (PreparedStatement cached = connection.prepareStatement(
                "insert into " + tableName + "(id, val) values(?, ?)")) {
            jdbc.executeUpdate(connection, "alter table " + tableName + " drop constraint " + constraintName);
            jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(1, -1)");
            assertThat(rowCount(connection, tableName, "id = 1")).isEqualTo(1);

            setInts(cached, 2, -2);
            assertCachedPreparedUpdateSucceeds(cached);
            assertThat(rowCount(connection, tableName, "id = 2")).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("DEFECT-TRG-001 Created trigger invalidates cached INSERT statements")
    void createdTriggerInvalidatesCachedInsertStatements() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TRG_CREATE_T");
        String triggerName = DbTestSupport.uniqueName("QA_DEF_TRG_CREATE");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");

        try (PreparedStatement cached = connection.prepareStatement(
                "insert into " + tableName + "(id, val) values(?, ?)")) {
            setInts(cached, 1, null);
            cached.executeUpdate();
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 1")).isNull();

            createBeforeInsertValueTrigger(triggerName, tableName, 0);
            jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(2, null)");
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 2"))
                    .isEqualTo("0");

            setInts(cached, 3, null);
            cached.executeUpdate();
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 3"))
                    .as("cached INSERT must execute the newly created trigger")
                    .isEqualTo("0");
        }
    }

    @Test
    @DisplayName("DEFECT-TRG-002 Dropped trigger invalidates cached INSERT statements")
    void droppedTriggerInvalidatesCachedInsertStatements() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TRG_DROP_T");
        String triggerName = DbTestSupport.uniqueName("QA_DEF_TRG_DROP");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        createBeforeInsertValueTrigger(triggerName, tableName, 0);

        try (PreparedStatement cached = connection.prepareStatement(
                "insert into " + tableName + "(id, val) values(?, ?)")) {
            setInts(cached, 1, null);
            cached.executeUpdate();
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 1"))
                    .isEqualTo("0");

            jdbc.executeUpdate(connection, "drop trigger " + triggerName);
            jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(2, null)");
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 2")).isNull();

            setInts(cached, 3, null);
            cached.executeUpdate();
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 3"))
                    .as("cached INSERT must not execute a dropped trigger")
                    .isNull();
        }
    }

    @Test
    @DisplayName("DEFECT-TRG-003 Replaced trigger body invalidates cached INSERT statements")
    void replacedTriggerBodyInvalidatesCachedInsertStatements() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TRG_REPL_T");
        String triggerName = DbTestSupport.uniqueName("QA_DEF_TRG_REPL");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        createBeforeInsertValueTrigger(triggerName, tableName, 10);

        try (PreparedStatement cached = connection.prepareStatement(
                "insert into " + tableName + "(id, val) values(?, ?)")) {
            setInts(cached, 1, null);
            cached.executeUpdate();
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 1"))
                    .isEqualTo("10");

            createBeforeInsertValueTrigger(triggerName, tableName, 20);
            jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(2, null)");
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 2"))
                    .isEqualTo("20");

            setInts(cached, 3, null);
            cached.executeUpdate();
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 3"))
                    .as("cached INSERT must execute the replaced trigger body")
                    .isEqualTo("20");
        }
    }

    @Test
    @DisplayName("DEFECT-TRG-004 Enabled trigger invalidates cached INSERT statements")
    void enabledTriggerInvalidatesCachedInsertStatements() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_TRG_ENABLE_T");
        String triggerName = DbTestSupport.uniqueName("QA_DEF_TRG_ENABLE");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        createBeforeInsertValueTriggerDisabled(triggerName, tableName, 0);

        try (PreparedStatement cached = connection.prepareStatement(
                "insert into " + tableName + "(id, val) values(?, ?)")) {
            setInts(cached, 1, null);
            cached.executeUpdate();
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 1")).isNull();

            jdbc.executeUpdate(connection, "alter trigger " + triggerName + " enable");
            jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(2, null)");
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 2"))
                    .isEqualTo("0");

            setInts(cached, 3, null);
            cached.executeUpdate();
            assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 3"))
                    .as("cached INSERT must honor the enabled trigger")
                    .isEqualTo("0");
        }
    }

    @Test
    @DisplayName("DEFECT-CALL-001 Procedure synonym retarget invalidates cached callables")
    void procedureSynonymRetargetInvalidatesCachedCallables() throws Exception {
        String firstOwner = createManagedUser("QA_DEF_CALL_PROC_A");
        String secondOwner = createManagedUser("QA_DEF_CALL_PROC_B");
        String consumer = createManagedUser("QA_DEF_CALL_PROC_C");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_CALL_PROC_P");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_CALL_PROC_SYN");

        withUserConnection(firstOwner, conn -> createOutProcedureReturning(conn, procedureName, 10));
        withUserConnection(secondOwner, conn -> createOutProcedureReturning(conn, procedureName, 20));
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant execute on " + firstOwner + "." + procedureName + " to " + consumer);
        jdbc.executeUpdate(connection, "grant execute on " + secondOwner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + firstOwner + "." + procedureName);

            try (CallableStatement cached = conn.prepareCall("{call " + synonymName + "(?)}")) {
                cached.registerOutParameter(1, Types.INTEGER);
                cached.execute();
                assertThat(cached.getInt(1)).isEqualTo(10);

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + secondOwner + "." + procedureName);
                assertThat(executeOutProcedure(conn, synonymName)).isEqualTo(20);

                assertCachedOutProcedureYieldsFreshOrFails(cached, 20);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-CALL-002 Function synonym retarget invalidates cached callables")
    void functionSynonymRetargetInvalidatesCachedCallables() throws Exception {
        String firstOwner = createManagedUser("QA_DEF_CALL_FUNC_A");
        String secondOwner = createManagedUser("QA_DEF_CALL_FUNC_B");
        String consumer = createManagedUser("QA_DEF_CALL_FUNC_C");
        String functionName = DbTestSupport.uniqueName("QA_DEF_CALL_FUNC_F");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_CALL_FUNC_SYN");

        withUserConnection(firstOwner, conn -> createFunctionMultiplier(conn, functionName, 2));
        withUserConnection(secondOwner, conn -> createFunctionMultiplier(conn, functionName, 3));
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant execute on " + firstOwner + "." + functionName + " to " + consumer);
        jdbc.executeUpdate(connection, "grant execute on " + secondOwner + "." + functionName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + firstOwner + "." + functionName);

            try (CallableStatement cached = conn.prepareCall("{ ? = call " + synonymName + "(?) }")) {
                cached.registerOutParameter(1, Types.INTEGER);
                cached.setInt(2, 21);
                cached.execute();
                assertThat(cached.getInt(1)).isEqualTo(42);

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + secondOwner + "." + functionName);
                assertThat(executeFunction(conn, synonymName, 21)).isEqualTo(63);

                assertCachedFunctionYieldsFreshOrFails(cached, 21, 63);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-CALL-003 Dropped procedure synonym invalidates cached callables")
    void droppedProcedureSynonymInvalidatesCachedCallables() throws Exception {
        String owner = createManagedUser("QA_DEF_CALL_DROP_O");
        String consumer = createManagedUser("QA_DEF_CALL_DROP_C");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_CALL_DROP_P");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_CALL_DROP_SYN");

        withUserConnection(owner, conn -> createOutProcedureReturning(conn, procedureName, 10));
        grantIfNeeded("create synonym", consumer);
        jdbc.executeUpdate(connection, "grant execute on " + owner + "." + procedureName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            jdbc.executeUpdate(conn, "create synonym " + synonymName + " for " + owner + "." + procedureName);

            try (CallableStatement cached = conn.prepareCall("{call " + synonymName + "(?)}")) {
                cached.registerOutParameter(1, Types.INTEGER);
                cached.execute();
                assertThat(cached.getInt(1)).isEqualTo(10);

                jdbc.executeUpdate(conn, "drop synonym " + synonymName);
                assertThatThrownBy(() -> executeOutProcedure(conn, synonymName))
                        .isInstanceOf(SQLException.class);

                assertCachedOutProcedureFails(cached);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-SEQ-001 Sequence synonym retarget invalidates cached prepared statements")
    void sequenceSynonymRetargetInvalidatesCachedPreparedStatements() throws Exception {
        String firstSequence = DbTestSupport.uniqueName("QA_DEF_SEQ_A");
        String secondSequence = DbTestSupport.uniqueName("QA_DEF_SEQ_B");
        String synonymName = DbTestSupport.uniqueName("QA_DEF_SEQ_SYN");
        registerCleanup(() -> DbTestSupport.dropSynonymQuietly(jdbc, connection, synonymName));
        registerCleanup(() -> dropSequenceQuietly(secondSequence));
        registerCleanup(() -> dropSequenceQuietly(firstSequence));

        jdbc.executeUpdate(connection, "create sequence " + firstSequence + " start with 100 increment by 1");
        jdbc.executeUpdate(connection, "create sequence " + secondSequence + " start with 200 increment by 1");
        jdbc.executeUpdate(connection, "create synonym " + synonymName + " for " + firstSequence);

        try (PreparedStatement cached = connection.prepareStatement("select " + synonymName + ".nextval from dual")) {
            assertThat(singleString(cached)).isEqualTo("100");

            jdbc.executeUpdate(connection, "drop synonym " + synonymName);
            jdbc.executeUpdate(connection, "create synonym " + synonymName + " for " + secondSequence);
            assertThat(jdbc.queryForString(connection, "select " + synonymName + ".nextval from dual")).isEqualTo("200");

            assertCachedPreparedStatementYieldsFreshOrFails(cached, "201");
        }
    }

    @Test
    @DisplayName("DEFECT-SEQ-002 Dropped sequence invalidates cached prepared statements")
    void droppedSequenceInvalidatesCachedPreparedStatements() throws Exception {
        String sequenceName = DbTestSupport.uniqueName("QA_DEF_SEQ_DROP");
        registerCleanup(() -> dropSequenceQuietly(sequenceName));

        jdbc.executeUpdate(connection, "create sequence " + sequenceName + " start with 1 increment by 1");

        try (PreparedStatement cached = connection.prepareStatement("select " + sequenceName + ".nextval from dual")) {
            assertThat(singleString(cached)).isEqualTo("1");

            jdbc.executeUpdate(connection, "drop sequence " + sequenceName);
            assertThatThrownBy(() -> jdbc.queryForString(connection, "select " + sequenceName + ".nextval from dual"))
                    .isInstanceOf(IllegalStateException.class);

            assertCachedPreparedStatementFails(cached);
        }
    }

    @Test
    @DisplayName("DEFECT-PRIV-001 Revoked UPDATE privilege invalidates cached prepared statements")
    void revokedUpdatePrivilegeInvalidatesCachedPreparedStatements() throws Exception {
        String owner = createManagedUser("QA_DEF_PRIV_UPD_O");
        String consumer = createManagedUser("QA_DEF_PRIV_UPD_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_PRIV_UPD_T");

        createDmlTarget(owner, tableName, 1, 10);
        insertDmlRow(owner, tableName, 2, 20);
        jdbc.executeUpdate(connection, "grant update on " + owner + "." + tableName + " to " + consumer);
        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (PreparedStatement cached = conn.prepareStatement(
                    "update " + owner + "." + tableName + " set val = ? where id = ?")) {
                setInts(cached, 11, 1);
                cached.executeUpdate();
                assertThat(jdbc.queryForString(connection,
                        "select val from " + owner + "." + tableName + " where id = 1")).isEqualTo("11");

                jdbc.executeUpdate(connection, "revoke update on " + owner + "." + tableName + " from " + consumer);
                assertThatThrownBy(() ->
                        jdbc.executeUpdate(conn, "update " + owner + "." + tableName + " set val = 21 where id = 2"))
                        .isInstanceOf(IllegalStateException.class);

                setInts(cached, 22, 2);
                assertCachedPreparedUpdateFails(cached);
                assertThat(jdbc.queryForString(connection,
                        "select val from " + owner + "." + tableName + " where id = 2")).isEqualTo("20");
            }
        });
    }

    @Test
    @DisplayName("DEFECT-PRIV-002 Revoked DELETE privilege invalidates cached prepared statements")
    void revokedDeletePrivilegeInvalidatesCachedPreparedStatements() throws Exception {
        String owner = createManagedUser("QA_DEF_PRIV_DEL_O");
        String consumer = createManagedUser("QA_DEF_PRIV_DEL_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_PRIV_DEL_T");

        createDmlTarget(owner, tableName, 1, 10);
        insertDmlRow(owner, tableName, 2, 20);
        jdbc.executeUpdate(connection, "grant delete on " + owner + "." + tableName + " to " + consumer);
        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to " + consumer);

        withUserConnection(consumer, conn -> {
            try (PreparedStatement cached = conn.prepareStatement(
                    "delete from " + owner + "." + tableName + " where id = ?")) {
                cached.setInt(1, 1);
                cached.executeUpdate();
                assertThat(rowCount(connection, owner + "." + tableName, "id = 1")).isEqualTo(0);

                jdbc.executeUpdate(connection, "revoke delete on " + owner + "." + tableName + " from " + consumer);
                assertThatThrownBy(() ->
                        jdbc.executeUpdate(conn, "delete from " + owner + "." + tableName + " where id = 2"))
                        .isInstanceOf(IllegalStateException.class);

                cached.setInt(1, 2);
                assertCachedPreparedUpdateFails(cached);
                assertThat(rowCount(connection, owner + "." + tableName, "id = 2")).isEqualTo(1);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-PRIV-003 Revoked PUBLIC SELECT invalidates cached prepared statements")
    void revokedPublicSelectInvalidatesCachedPreparedStatements() throws Exception {
        String owner = createManagedUser("QA_DEF_PRIV_PUB_O");
        String consumer = createManagedUser("QA_DEF_PRIV_PUB_C");
        String tableName = DbTestSupport.uniqueName("QA_DEF_PRIV_PUB_T");

        createDmlTarget(owner, tableName, 1, 10);
        jdbc.executeUpdate(connection, "grant select on " + owner + "." + tableName + " to public");

        withUserConnection(consumer, conn -> {
            try (PreparedStatement cached = conn.prepareStatement(
                    "select val from " + owner + "." + tableName + " where id = 1")) {
                assertThat(singleString(cached)).isEqualTo("10");

                jdbc.executeUpdate(connection, "revoke select on " + owner + "." + tableName + " from public");
                assertThatThrownBy(() ->
                        jdbc.queryForString(conn, "select val from " + owner + "." + tableName + " where id = 1"))
                        .isInstanceOf(IllegalStateException.class);

                assertCachedPreparedStatementFails(cached);
            }
        });
    }

    @Test
    @DisplayName("DEFECT-VIEW-001 Dropped view invalidates cached prepared statements")
    void droppedViewInvalidatesCachedPreparedStatements() throws Exception {
        String viewName = DbTestSupport.uniqueName("QA_DEF_VIEW_DROP");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, viewName));

        jdbc.executeUpdate(connection, "create view " + viewName + " as select 10 as c1 from dual");

        try (PreparedStatement cached = connection.prepareStatement("select c1 from " + viewName)) {
            assertThat(singleString(cached)).isEqualTo("10");

            jdbc.executeUpdate(connection, "drop view " + viewName);
            assertThatThrownBy(() -> jdbc.queryForString(connection, "select c1 from " + viewName))
                    .isInstanceOf(IllegalStateException.class);

            assertCachedPreparedStatementFails(cached);
        }
    }

    @Test
    @DisplayName("DEFECT-IDX-001 Cached predicate results remain stable across index create and drop")
    void cachedPredicateResultsRemainStableAcrossIndexCreateAndDrop() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_IDX_CACHE_T");
        String indexName = DbTestSupport.uniqueName("QA_DEF_IDX_CACHE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 20)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, null)");

        try (PreparedStatement cached = connection.prepareStatement(
                "select count(*) from " + tableName + " where val >= ? or val is null")) {
            cached.setInt(1, 20);
            assertThat(singleString(cached)).isEqualTo("2");

            jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(val)");
            cached.setInt(1, 20);
            assertThat(singleString(cached)).isEqualTo("2");

            jdbc.executeUpdate(connection, "drop index " + indexName);
            cached.setInt(1, 20);
            assertThat(singleString(cached)).isEqualTo("2");
        }
    }

    @Test
    @DisplayName("DEFECT-ATOMIC-001 Failed CHECK constraint addition leaves no partial constraint")
    void failedCheckConstraintAdditionLeavesNoPartialConstraint() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_ATOM_CHECK");
        String constraintName = DbTestSupport.uniqueName("QA_DEF_ATOM_CK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, -1)");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "alter table " + tableName + " add constraint " + constraintName + " check(val > 0)"))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, -2)");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName + " where val < 0"))
                .isEqualTo("2");
    }

    @Test
    @DisplayName("DEFECT-ATOMIC-002 Failed unique index creation leaves no partial index metadata")
    void failedUniqueIndexCreationLeavesNoPartialIndexMetadata() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_ATOM_UIDX");
        String indexName = DbTestSupport.uniqueName("QA_DEF_ATOM_UIDX_I");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 10)");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "create unique index " + indexName + " on " + tableName + "(val)"))
                .isInstanceOf(IllegalStateException.class);

        assertThat(indexExists(indexName)).isFalse();
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 10)");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName + " where val = 10"))
                .isEqualTo("3");
    }

    @Test
    @DisplayName("DEFECT-ATOMIC-003 Failed procedure replacement keeps previous valid body")
    void failedProcedureReplacementKeepsPreviousValidBody() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_DEF_ATOM_PROC");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createOutProcedureReturning(connection, procedureName, 11);
        assertThat(executeOutProcedure(connection, procedureName)).isEqualTo(11);

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "create or replace procedure " + procedureName +
                        "(p1 out integer) as begin p1 := ; end;"))
                .isInstanceOf(IllegalStateException.class);

        assertThat(executeOutProcedure(connection, procedureName))
                .as("failed CREATE OR REPLACE PROCEDURE must not replace or drop the last valid procedure body")
                .isEqualTo(11);
    }

    @Test
    @DisplayName("DEFECT-ATOMIC-004 Failed function replacement keeps previous valid body")
    void failedFunctionReplacementKeepsPreviousValidBody() throws Exception {
        String functionName = DbTestSupport.uniqueName("QA_DEF_ATOM_FUNC");
        registerCleanup(() -> DbTestSupport.dropFunctionQuietly(jdbc, connection, functionName));

        createFunctionMultiplier(connection, functionName, 2);
        assertThat(executeFunction(connection, functionName, 7)).isEqualTo(14);

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "create or replace function " + functionName +
                        "(p1 in integer) return integer as begin return ; end;"))
                .isInstanceOf(IllegalStateException.class);

        assertThat(executeFunction(connection, functionName, 7))
                .as("failed CREATE OR REPLACE FUNCTION must not replace or drop the last valid function body")
                .isEqualTo(14);
    }

    @Test
    @DisplayName("DEFECT-ATOMIC-005 Failed view replacement keeps previous valid definition")
    void failedViewReplacementKeepsPreviousValidDefinition() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_ATOM_VIEW_T");
        String viewName = DbTestSupport.uniqueName("QA_DEF_ATOM_VIEW");
        registerCleanup(() -> DbTestSupport.dropViewQuietly(jdbc, connection, viewName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 33)");
        jdbc.executeUpdate(connection, "create or replace view " + viewName +
                " as select val from " + tableName + " where id = 1");
        assertThat(jdbc.queryForString(connection, "select val from " + viewName)).isEqualTo("33");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "create or replace view " + viewName + " as select missing_column from " + tableName))
                .isInstanceOf(IllegalStateException.class);

        assertThat(jdbc.queryForString(connection, "select val from " + viewName))
                .as("failed CREATE OR REPLACE VIEW must preserve the last valid view definition")
                .isEqualTo("33");
    }

    @Test
    @DisplayName("DEFECT-ATOMIC-006 Failed trigger replacement keeps previous valid body")
    void failedTriggerReplacementKeepsPreviousValidBody() {
        String tableName = DbTestSupport.uniqueName("QA_DEF_ATOM_TRG_T");
        String triggerName = DbTestSupport.uniqueName("QA_DEF_ATOM_TRG");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, val integer)");
        createBeforeInsertValueTrigger(triggerName, tableName, 44);
        jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(1, null)");
        assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 1"))
                .isEqualTo("44");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "create or replace trigger " + triggerName + " before insert on " + tableName +
                        " referencing new row new_row for each row as begin new_row.val := ; end;"))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "insert into " + tableName + "(id, val) values(2, null)");
        assertThat(jdbc.queryForString(connection, "select val from " + tableName + " where id = 2"))
                .as("failed CREATE OR REPLACE TRIGGER must preserve the last valid trigger body")
                .isEqualTo("44");
    }

    @Test
    @DisplayName("DEFECT-REPL-001 Drop replication removes metadata after add host and add table")
    void dropReplicationRemovesMetadataAfterAddHostAndAddTable() {
        FeatureProbe.assumeReplicationAvailable(config, jdbc, connection);

        String firstTable = createReplicatedTable("QA_DEF_REPL_ONE");
        String secondTable = createReplicatedTable("QA_DEF_REPL_TWO");
        String replicationName = DbTestSupport.uniqueName("QA_DEF_REPL");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(firstTable));
        jdbc.executeUpdate(connection,
                "alter replication " + replicationName + " add table " + tableMappingClause(secondTable));
        jdbc.executeUpdate(connection,
                "alter replication " + replicationName + " add host '127.0.0.2', " + replicationPort());

        assertThat(jdbc.queryForString(connection,
                "select item_count from system_.sys_replications_ where replication_name = '" + replicationName + "'"))
                .isEqualTo("2");
        assertThat(jdbc.queryForString(connection,
                "select host_count from system_.sys_replications_ where replication_name = '" + replicationName + "'"))
                .isEqualTo("2");

        jdbc.executeUpdate(connection, "drop replication " + replicationName);

        assertThat(jdbc.queryForString(connection,
                "select count(*) from system_.sys_replications_ where replication_name = '" + replicationName + "'"))
                .isEqualTo("0");
    }

    @Test
    @DisplayName("DEFECT-REPL-002 Replication DDL error does not poison the session")
    void replicationDdlErrorDoesNotPoisonSession() {
        FeatureProbe.assumeReplicationAvailable(config, jdbc, connection);

        String tableName = createReplicatedTable("QA_DEF_REPL_ERR");
        String sanityTable = DbTestSupport.uniqueName("QA_DEF_REPL_SANITY");
        String replicationName = DbTestSupport.uniqueName("QA_DEF_REPL_ERR");
        registerCleanup(() -> dropReplicationQuietly(replicationName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, sanityTable));

        jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName));

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "alter replication " + replicationName + " add table " + tableMappingClause(tableName)))
                .isInstanceOf(IllegalStateException.class);

        jdbc.executeUpdate(connection, "create table " + sanityTable + "(id integer primary key)");
        jdbc.executeUpdate(connection, "insert into " + sanityTable + " values(1)");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + sanityTable)).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-REPL-003 Same-name replication recreate does not reuse stale metadata")
    void sameNameReplicationRecreateDoesNotReuseStaleMetadata() {
        FeatureProbe.assumeReplicationAvailable(config, jdbc, connection);

        String firstTable = createReplicatedTable("QA_DEF_REPL_REUSE_ONE");
        String secondTable = createReplicatedTable("QA_DEF_REPL_REUSE_TWO");
        String replicationName = DbTestSupport.uniqueName("QA_DEF_REPL_REUSE");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(firstTable));
        jdbc.executeUpdate(connection,
                "alter replication " + replicationName + " add host '127.0.0.2', " + replicationPort());
        assertReplicationCounts(replicationName, 1, 2);

        jdbc.executeUpdate(connection, "drop replication " + replicationName);
        assertThat(replicationCount(replicationName)).isEqualTo(0);

        jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(secondTable));

        assertReplicationCounts(replicationName, 1, 1);
    }

    @Test
    @DisplayName("DEFECT-REPL-004 Failed create leaves no replication metadata and same name can be reused")
    void failedCreateLeavesNoReplicationMetadataAndSameNameCanBeReused() {
        FeatureProbe.assumeReplicationAvailable(config, jdbc, connection);

        String invalidTable = DbTestSupport.uniqueName("QA_DEF_REPL_NOPK");
        String validTable = createReplicatedTable("QA_DEF_REPL_VALID");
        String replicationName = DbTestSupport.uniqueName("QA_DEF_REPL_FAIL_CREATE");
        registerCleanup(() -> dropReplicationQuietly(replicationName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, invalidTable));

        jdbc.executeUpdate(connection, "create table " + invalidTable + "(id integer)");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(invalidTable)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(replicationCount(replicationName)).isEqualTo(0);

        jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(validTable));

        assertReplicationCounts(replicationName, 1, 1);
    }

    @Test
    @DisplayName("DEFECT-REPL-005 Duplicate table and host errors preserve replication metadata")
    void duplicateTableAndHostErrorsPreserveReplicationMetadata() {
        FeatureProbe.assumeReplicationAvailable(config, jdbc, connection);

        String tableName = createReplicatedTable("QA_DEF_REPL_DUP_META");
        String replicationName = DbTestSupport.uniqueName("QA_DEF_REPL_DUP_META");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName));
        assertReplicationCounts(replicationName, 1, 1);

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "alter replication " + replicationName + " add table " + tableMappingClause(tableName)))
                .isInstanceOf(IllegalStateException.class);
        assertReplicationCounts(replicationName, 1, 1);

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "alter replication " + replicationName + " add host '" +
                        config.replication().remoteHost() + "', " + replicationPort()))
                .isInstanceOf(IllegalStateException.class);
        assertReplicationCounts(replicationName, 1, 1);
    }

    @Test
    @DisplayName("DEFECT-REPL-006 Drop-table error after metadata mutation preserves item count")
    void dropTableErrorAfterMetadataMutationPreservesItemCount() {
        FeatureProbe.assumeReplicationAvailable(config, jdbc, connection);

        String firstTable = createReplicatedTable("QA_DEF_REPL_DROP_KEEP_ONE");
        String secondTable = createReplicatedTable("QA_DEF_REPL_DROP_KEEP_TWO");
        String thirdTable = createReplicatedTable("QA_DEF_REPL_DROP_KEEP_THREE");
        String replicationName = DbTestSupport.uniqueName("QA_DEF_REPL_DROP_KEEP");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(firstTable, secondTable));
        assertReplicationCounts(replicationName, 2, 1);

        jdbc.executeUpdate(connection,
                "alter replication " + replicationName + " drop table " + tableMappingClause(secondTable));
        assertReplicationCounts(replicationName, 1, 1);

        assertThatThrownBy(() -> jdbc.executeUpdate(connection,
                "alter replication " + replicationName + " drop table " + tableMappingClause(thirdTable)))
                .isInstanceOf(IllegalStateException.class);
        assertReplicationCounts(replicationName, 1, 1);
    }

    @Test
    @DisplayName("DEFECT-REPL-007 Self-hosted start failure leaves replication stopped and droppable")
    void selfHostedStartFailureLeavesReplicationStoppedAndDroppable() {
        FeatureProbe.assumeReplicationAvailable(config, jdbc, connection);

        String tableName = createReplicatedTable("QA_DEF_REPL_START_FAIL");
        String replicationName = DbTestSupport.uniqueName("QA_DEF_REPL_START_FAIL");
        registerCleanup(() -> dropReplicationQuietly(replicationName));

        jdbc.executeUpdate(connection,
                "create replication " + replicationName + " " + selfHostedWithClause() + " " +
                        replicationTableClause(tableName));

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "alter replication " + replicationName + " start"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(replicationIsStarted(replicationName)).isEqualTo(0);

        jdbc.executeUpdate(connection, "drop replication " + replicationName);
        assertThat(replicationCount(replicationName)).isEqualTo(0);
    }

    @Test
    @DisplayName("DEFECT-BR-001 CHECKPOINT does not commit or expose uncommitted data")
    void checkpointDoesNotCommitOrExposeUncommittedData() {
        FeatureProbe.assumeRecoveryEnabled(config);

        String tableName = DbTestSupport.uniqueName("QA_DEF_BR_CP_ISO");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");

        Connection observer = jdbc.open();
        jdbc.begin(connection);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

            executeAsSysdba("alter system checkpoint");

            assertThat(jdbc.queryForString(observer, "select count(*) from " + tableName)).isEqualTo("0");
            jdbc.commit(connection);
        } finally {
            rollbackQuietly(connection);
            jdbc.closeQuietly(observer);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-BR-002 CHECKPOINT preserves rollback-to-savepoint behavior")
    void checkpointPreservesRollbackToSavepointBehavior() throws Exception {
        FeatureProbe.assumeRecoveryEnabled(config);

        String tableName = DbTestSupport.uniqueName("QA_DEF_BR_CP_SP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
        connection.setAutoCommit(false);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            var savepoint = connection.setSavepoint("QA_DEF_BR_SP");
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");

            executeAsSysdba("alter system checkpoint");

            connection.rollback(savepoint);
            connection.commit();
        } finally {
            rollbackQuietly(connection);
        }

        assertThat(orderedRows("select id from " + tableName + " order by id"))
                .containsExactly("1");
    }

    @Test
    @DisplayName("DEFECT-BR-003 Service-phase recovery rejection leaves SYSDBA and normal sessions usable")
    void servicePhaseRecoveryRejectionLeavesSessionsUsable() {
        FeatureProbe.assumeRecoveryEnabled(config);

        assertSysdbaUpdateFails("alter database recover database");

        executeAsSysdba("alter system checkpoint");
        assertThat(jdbc.queryForString(connection, "select count(*) from dual")).isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-BR-004 Failed backup command does not affect an active user transaction")
    void failedBackupCommandDoesNotAffectActiveUserTransaction() {
        FeatureProbe.assumeRecoveryEnabled(config);

        String tableName = DbTestSupport.uniqueName("QA_DEF_BR_BK_TX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
        jdbc.begin(connection);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

            assertSysdbaUpdateFails("alter database backup tablespace QA_DEF_BR_MISSING_TBS to '" +
                    config.paths().backupDir() + "'");

            jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
            jdbc.commit(connection);
        } finally {
            rollbackQuietly(connection);
        }

        assertThat(orderedRows("select id from " + tableName + " order by id"))
                .containsExactly("1", "2");
    }

    @Test
    @DisplayName("DEFECT-BR-005 Backup failure on a dropped tablespace does not leave stale metadata")
    void backupFailureOnDroppedTablespaceDoesNotLeaveStaleMetadata() {
        FeatureProbe.assumeRecoveryEnabled(config);

        String tablespaceName = DbTestSupport.uniqueName("QA_DEF_BR_DROP_TBS");
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));

        jdbc.executeUpdate(connection, "create memory data tablespace " + tablespaceName + " size 4M");
        assertThat(tablespaceExists(tablespaceName)).isTrue();

        jdbc.executeUpdate(connection, "drop tablespace " + tablespaceName);
        assertThat(tablespaceExists(tablespaceName)).isFalse();

        assertSysdbaUpdateFails("alter database backup tablespace " + tablespaceName +
                " to '" + config.paths().backupDir() + "'");
        executeAsSysdba("alter system checkpoint");
        assertThat(tablespaceExists(tablespaceName)).isFalse();
    }

    @Test
    @DisplayName("DEFECT-BR-006 BEGIN/END BACKUP failure paths leave a memory tablespace usable")
    void beginEndBackupFailurePathsLeaveMemoryTablespaceUsable() {
        FeatureProbe.assumeRecoveryEnabled(config);

        String tablespaceName = DbTestSupport.uniqueName("QA_DEF_BR_BACKUP_STATE");
        registerCleanup(() -> endBackupQuietly(tablespaceName));
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));

        jdbc.executeUpdate(connection, "create memory data tablespace " + tablespaceName + " size 4M");

        if (isArchiveLogMode()) {
            executeAsSysdba("alter tablespace " + tablespaceName + " begin backup");
            executeAsSysdba("alter tablespace " + tablespaceName + " end backup");
        } else {
            assertSysdbaUpdateFails("alter tablespace " + tablespaceName + " begin backup");
        }

        jdbc.executeUpdate(connection, "create table " + DbTestSupport.uniqueName("QA_DEF_BR_BACKUP_T") +
                "(id integer primary key) tablespace " + tablespaceName);
        assertThat(tablespaceExists(tablespaceName)).isTrue();
    }

    @Test
    @DisplayName("DEFECT-PKG-001 UTL_RAW integer conversions round-trip representative values")
    void utlRawIntegerConversionsRoundTripRepresentativeValues() {
        FeatureProbe.assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "UTL_RAW",
                "select utl_raw.cast_from_binary_integer(1) from dual"
        );

        int[] values = {0, 1, 255, 65_535, 123_456};
        for (int value : values) {
            assertThat(jdbc.queryForString(connection,
                    "select utl_raw.cast_to_binary_integer(utl_raw.cast_from_binary_integer(" + value + ")) from dual"))
                    .isEqualTo(String.valueOf(value));
        }
    }

    @Test
    @DisplayName("DEFECT-PKG-002 Failed UTL_RAW conversion does not poison later package calls")
    void failedUtlRawConversionDoesNotPoisonLaterPackageCalls() {
        FeatureProbe.assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "UTL_RAW",
                "select utl_raw.cast_from_binary_integer(1) from dual"
        );

        assertThatThrownBy(() ->
                jdbc.queryForString(connection, "select utl_raw.cast_to_binary_integer('ABC') from dual"))
                .isInstanceOf(IllegalStateException.class)
                .satisfies(throwable -> assertThat((Object) SqlExceptionSupport.findSqlException(throwable)).isNotNull());

        assertThat(jdbc.queryForString(connection,
                "select utl_raw.cast_to_binary_integer(utl_raw.cast_from_binary_integer(123456)) from dual"))
                .isEqualTo("123456");
    }

    @Test
    @DisplayName("DEFECT-PKG-003 DBMS_RANDOM.VALUE stays inside the requested numeric range")
    void dbmsRandomValueStaysInsideRequestedRange() {
        FeatureProbe.assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "DBMS_RANDOM",
                "select dbms_random.value(10, 20) from dual"
        );

        for (int index = 0; index < 20; index++) {
            double value = Double.parseDouble(jdbc.queryForString(connection,
                    "select dbms_random.value(10, 20) from dual"));
            assertThat(value).isGreaterThanOrEqualTo(10.0).isLessThanOrEqualTo(20.0);
        }
    }

    @Test
    @DisplayName("DEFECT-PKG-004 Failed TCP connection does not poison later UTL_TCP echo writes")
    void failedTcpConnectionDoesNotPoisonLaterEchoWrites() throws Exception {
        FeatureProbe.assumeUtlTcpAvailable(config, jdbc, connection);

        String probeProcedure = DbTestSupport.uniqueName("QA_DEF_PKG_TCP_FAIL");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, probeProcedure));

        jdbc.executeUpdate(connection,
                "create or replace procedure " + probeProcedure + "(p_connected out integer, p_closed out integer) as " +
                        "c connect_type; " +
                        "begin " +
                        "c := open_connect('127.0.0.1', 1, 100, 100); " +
                        "p_connected := is_connected(c); " +
                        "p_closed := close_connect(c); " +
                        "end;");

        int[] failedConnectResult = callTcpConnectionStateProcedure(probeProcedure);
        assertThat(failedConnectResult[0]).isNotEqualTo(0);

        int[] echoResult = callTcpEchoProcedure("QA_DEF_PKG_TCP_AFTER_FAIL", "AFTER_FAIL");
        assertThat(echoResult[0]).isEqualTo(0);
        assertThat(echoResult[1]).isGreaterThan(0);
        assertThat(echoResult[2]).isEqualTo(0);
    }

    @Test
    @DisplayName("DEFECT-PKG-005 Closed TCP handles cannot be reused and later handles still work")
    void closedTcpHandlesCannotBeReusedAndLaterHandlesStillWork() throws Exception {
        FeatureProbe.assumeUtlTcpAvailable(config, jdbc, connection);

        String probeProcedure = DbTestSupport.uniqueName("QA_DEF_PKG_TCP_CLOSED");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, probeProcedure));

        jdbc.executeUpdate(connection,
                "create or replace procedure " + probeProcedure + "(p_closed out integer, p_written_after_close out integer) as " +
                        "c connect_type; " +
                        "begin " +
                        "c := open_connect('" + config.network().tcpHost() + "', " + config.network().tcpPort() + ", 1000, 3000); " +
                        "p_closed := close_connect(c); " +
                        "p_written_after_close := write_raw(c, to_raw('AFTER_CLOSE' || chr(10)), raw_sizeof('AFTER_CLOSE' || chr(10))); " +
                        "end;");

        int[] closedHandleResult = callClosedTcpWriteProcedure(probeProcedure);
        assertThat(closedHandleResult[0]).isEqualTo(0);
        assertThat(closedHandleResult[1]).isLessThanOrEqualTo(0);

        int[] echoResult = callTcpEchoProcedure("QA_DEF_PKG_TCP_AFTER_CLOSE", "AFTER_CLOSE_OK");
        assertThat(echoResult[0]).isEqualTo(0);
        assertThat(echoResult[1]).isGreaterThan(0);
        assertThat(echoResult[2]).isEqualTo(0);
    }

    @Test
    @DisplayName("DEFECT-PKG-006 Closed SMTP sessions reject commands and later SMTP sends still work")
    void closedSmtpSessionsRejectCommandsAndLaterSmtpSendsStillWork() throws Exception {
        FeatureProbe.assumeUtlSmtpAvailable(config, jdbc, connection);

        String failingProcedure = DbTestSupport.uniqueName("QA_DEF_PKG_SMTP_CLOSED");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, failingProcedure));

        jdbc.executeUpdate(connection,
                "create or replace procedure " + failingProcedure + " as " +
                        "c connect_type; " +
                        "r varchar(65534); " +
                        "begin " +
                        "c := utl_smtp.open_connection('" + config.network().smtpHost() + "', " + config.network().smtpPort() + ", null); " +
                        "r := utl_smtp.quit(c); " +
                        "r := utl_smtp.mail(c, 'qa@example.com'); " +
                        "end;");

        assertProcedureFails(failingProcedure);

        assertThat(callSmtpSendProcedure("QA_DEF_PKG_SMTP_AFTER_CLOSE"))
                .contains("221");
    }

    @Test
    @DisplayName("DEFECT-PKG-007 DIRECTORY-backed file I/O preserves multiline payload order")
    void directoryBackedFileIoPreservesMultilinePayloadOrder() throws Exception {
        FeatureProbe.assumeDirectoryFileIoAvailable(config, jdbc, connection);

        String directoryName = DbTestSupport.uniqueName("QA_DEF_PKG_DIR");
        String procedureName = DbTestSupport.uniqueName("QA_DEF_PKG_DIR_RW");
        String fileName = procedureName.toLowerCase() + ".txt";
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create directory " + directoryName + " as '" + config.paths().workRoot() + "'");
        createDirectoryRoundTripProcedure(procedureName, directoryName, fileName);

        String[] lines = callDirectoryRoundTripProcedure(procedureName);
        assertThat(lines).containsExactly("FIRST_LINE", "SECOND_LINE");
    }

    @Test
    @DisplayName("DEFECT-PKG-008 Failed DIRECTORY read does not poison later file I/O")
    void failedDirectoryReadDoesNotPoisonLaterFileIo() throws Exception {
        FeatureProbe.assumeDirectoryFileIoAvailable(config, jdbc, connection);

        String directoryName = DbTestSupport.uniqueName("QA_DEF_PKG_DIR_FAIL");
        String failingProcedure = DbTestSupport.uniqueName("QA_DEF_PKG_DIR_MISSING");
        String workingProcedure = DbTestSupport.uniqueName("QA_DEF_PKG_DIR_AFTER");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, failingProcedure));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, workingProcedure));
        registerCleanup(() -> dropDirectoryQuietly(directoryName));

        jdbc.executeUpdate(connection, "create directory " + directoryName + " as '" + config.paths().workRoot() + "'");
        jdbc.executeUpdate(connection,
                "create or replace procedure " + failingProcedure + " as " +
                        "f FILE_TYPE; " +
                        "line varchar(200); " +
                        "begin " +
                        "f := FOPEN('" + directoryName + "', 'missing_" + failingProcedure.toLowerCase() + ".txt', 'R'); " +
                        "GET_LINE(f, line); " +
                        "FCLOSE(f); " +
                        "end;");

        assertProcedureFails(failingProcedure);

        createDirectoryRoundTripProcedure(workingProcedure, directoryName, workingProcedure.toLowerCase() + ".txt");
        assertThat(callDirectoryRoundTripProcedure(workingProcedure))
                .containsExactly("FIRST_LINE", "SECOND_LINE");
    }

    @Test
    @DisplayName("DEFECT-JDBC-001 PreparedStatement.clearParameters removes stale bindings")
    void preparedStatementClearParametersRemovesStaleBindings() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_CLEAR_PARAM");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, value integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 7)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 9)");

        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("select value from " + tableName + " where id = ?")) {
            preparedStatement.setInt(1, 1);
            assertThat(singleString(preparedStatement)).isEqualTo("7");

            preparedStatement.clearParameters();
            assertThatThrownBy(preparedStatement::executeQuery)
                    .isInstanceOf(SQLException.class);

            preparedStatement.setInt(1, 2);
            assertThat(singleString(preparedStatement)).isEqualTo("9");
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-002 Failed PreparedStatement batch can be cleared and reused")
    void failedPreparedStatementBatchCanBeClearedAndReused() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_BATCH");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");

        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("insert into " + tableName + " values(?)")) {
            preparedStatement.setInt(1, 1);
            preparedStatement.addBatch();
            preparedStatement.setInt(1, 1);
            preparedStatement.addBatch();

            assertThatThrownBy(preparedStatement::executeBatch)
                    .isInstanceOf(SQLException.class);

            preparedStatement.clearBatch();
            preparedStatement.clearParameters();
            preparedStatement.setInt(1, 2);
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        }

        assertThat(jdbc.queryForString(connection,
                "select count(*) from " + tableName + " where id = 2"))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-JDBC-003 ResultSet.wasNull is scoped to the last-read column")
    void resultSetWasNullIsScopedToLastReadColumn() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_WASNULL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c_null integer, c_value integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(null, 7)");

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select c_null, c_value from " + tableName)) {
            assertThat(resultSet.next()).isTrue();

            assertThat(resultSet.getInt("C_NULL")).isZero();
            assertThat(resultSet.wasNull()).isTrue();

            assertThat(resultSet.getInt("C_VALUE")).isEqualTo(7);
            assertThat(resultSet.wasNull()).isFalse();

            assertThat(resultSet.getString("C_NULL")).isNull();
            assertThat(resultSet.wasNull()).isTrue();
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-004 Closing one ResultSet does not close the owning Statement")
    void closingOneResultSetDoesNotCloseOwningStatement() throws Exception {
        try (Statement statement = connection.createStatement()) {
            ResultSet first = statement.executeQuery("select 1 as c1 from dual");
            assertThat(first.next()).isTrue();
            first.close();
            assertThat(first.isClosed()).isTrue();
            assertThat(statement.isClosed()).isFalse();

            try (ResultSet second = statement.executeQuery("select 2 as c1 from dual")) {
                assertThat(second.next()).isTrue();
                assertThat(second.getInt("C1")).isEqualTo(2);
            }
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-005 Statement.setMaxRows can be reset without stale row limiting")
    void statementMaxRowsCanBeResetWithoutStaleRowLimiting() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_MAXROWS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3)");

        try (Statement statement = connection.createStatement()) {
            statement.setMaxRows(2);
            assertThat(resultRowCount(statement, "select id from " + tableName + " order by id"))
                    .isEqualTo(2);

            statement.setMaxRows(0);
            assertThat(resultRowCount(statement, "select id from " + tableName + " order by id"))
                    .isEqualTo(3);
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-006 Failed ResultSet column access does not corrupt the cursor")
    void failedResultSetColumnAccessDoesNotCorruptCursor() throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select 42 as c1, 'ALTIBASE' as c2 from dual")) {
            assertThat(resultSet.next()).isTrue();
            assertThatThrownBy(() -> resultSet.getString("MISSING_COLUMN"))
                    .isInstanceOf(SQLException.class);

            assertThat(resultSet.getInt("C1")).isEqualTo(42);
            assertThat(resultSet.getString(2)).isEqualTo("ALTIBASE");
            assertThat(resultSet.getMetaData().getColumnLabel(1)).isEqualToIgnoringCase("C1");
            assertThat(resultSet.getMetaData().getColumnLabel(2)).isEqualToIgnoringCase("C2");
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-007 DatabaseMetaData reflects create and drop in the same connection")
    void databaseMetaDataReflectsCreateAndDropInSameConnection() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_META_T");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        assertThat(tableVisibleInMetadata(tableName)).isFalse();

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
        assertThat(tableVisibleInMetadata(tableName)).isTrue();

        jdbc.executeUpdate(connection, "drop table " + tableName + " cascade");
        assertThat(tableVisibleInMetadata(tableName)).isFalse();
    }

    @Test
    @DisplayName("DEFECT-JDBC-008 Invalid prepared parameter indexes do not poison later binding")
    void invalidPreparedParameterIndexesDoNotPoisonLaterBinding() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_PARAM_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, value integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 123)");

        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("select value from " + tableName + " where id = ?")) {
            assertThatThrownBy(() -> preparedStatement.setInt(0, 1))
                    .isInstanceOf(SQLException.class);
            assertThatThrownBy(() -> preparedStatement.setInt(2, 1))
                    .isInstanceOf(SQLException.class);

            preparedStatement.setInt(1, 1);
            assertThat(singleString(preparedStatement)).isEqualTo("123");
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-009 Statement result state switches cleanly between update and query")
    void statementResultStateSwitchesCleanlyBetweenUpdateAndQuery() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_STATE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");

        try (Statement statement = connection.createStatement()) {
            assertThat(statement.execute("insert into " + tableName + " values(1)")).isFalse();
            assertThat(statement.getUpdateCount()).isEqualTo(1);
            assertThat(statement.getResultSet()).isNull();

            assertThat(statement.execute("select count(*) as c1 from " + tableName)).isTrue();
            assertThat(statement.getUpdateCount()).isEqualTo(-1);
            try (ResultSet resultSet = statement.getResultSet()) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getInt("C1")).isEqualTo(1);
            }
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-010 Scrollable ResultSet request is honored after mixed navigation")
    void scrollableResultSetRequestIsHonoredAfterMixedNavigation() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_SCROLL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, label varchar(10))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'A')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'B')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 'C')");

        assertThat(connection.getMetaData().supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE))
                .as("driver advertises scroll-insensitive ResultSet support")
                .isTrue();

        try (Statement statement = connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
             ResultSet resultSet = statement.executeQuery("select id, label from " + tableName + " order by id")) {
            assertThat(resultSet.getType()).isEqualTo(ResultSet.TYPE_SCROLL_INSENSITIVE);
            assertThat(resultSet.absolute(3)).isTrue();
            assertThat(resultSet.getString("LABEL")).isEqualTo("C");
            assertThat(resultSet.previous()).isTrue();
            assertThat(resultSet.getString("LABEL")).isEqualTo("B");
            assertThat(resultSet.first()).isTrue();
            assertThat(resultSet.getString("LABEL")).isEqualTo("A");
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-011 ResultSetMetaData preserves projected aliases and readable type names")
    void resultSetMetaDataPreservesProjectedAliasesAndReadableTypeNames() throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "select cast(123 as integer) as c_int, cast('ALTIBASE' as varchar(20)) as c_text from dual")) {
            ResultSetMetaData metaData = resultSet.getMetaData();

            assertThat(metaData.getColumnCount()).isEqualTo(2);
            assertThat(metaData.getColumnLabel(1)).isEqualToIgnoringCase("C_INT");
            assertThat(metaData.getColumnLabel(2)).isEqualToIgnoringCase("C_TEXT");
            assertThat(metaData.getColumnTypeName(1)).isNotBlank();
            assertThat(metaData.getColumnTypeName(2)).isNotBlank();

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt("C_INT")).isEqualTo(123);
            assertThat(resultSet.getString("C_TEXT")).isEqualTo("ALTIBASE");
            assertThat(resultSet.next()).isFalse();
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-012 PreparedStatement recovers after type-mismatch binding failure")
    void preparedStatementRecoversAfterTypeMismatchBindingFailure() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_TYPE_BIND");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");

        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("insert into " + tableName + " values(?)")) {
            preparedStatement.setString(1, "NOT_A_NUMBER");
            assertThatThrownBy(preparedStatement::executeUpdate)
                    .as("binding a non-numeric string to an integer column must fail")
                    .isInstanceOf(SQLException.class);

            preparedStatement.clearParameters();
            preparedStatement.setInt(1, 1);
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName + " where id = 1"))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("DEFECT-JDBC-013 ResultSetMetaData reports numeric precision and scale")
    void resultSetMetaDataReportsNumericPrecisionAndScale() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_NUM_META");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(amount numeric(10,2))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(123.45)");

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select amount from " + tableName)) {
            ResultSetMetaData metaData = resultSet.getMetaData();

            assertThat(metaData.getPrecision(1)).isEqualTo(10);
            assertThat(metaData.getScale(1)).isEqualTo(2);
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).isEqualTo("123.45");
        }
    }

    @Test
    @DisplayName("DEFECT-JDBC-014 DatabaseMetaData reflects alter add and drop column")
    void databaseMetaDataReflectsAlterAddAndDropColumn() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_META_COL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        assertThat(columnVisibleInMetadata(connection, config.db().user(), tableName, "C1")).isTrue();
        assertThat(columnVisibleInMetadata(connection, config.db().user(), tableName, "C2")).isFalse();

        jdbc.executeUpdate(connection, "alter table " + tableName + " add column (c2 varchar(20))");
        assertThat(columnVisibleInMetadata(connection, config.db().user(), tableName, "C2")).isTrue();

        jdbc.executeUpdate(connection, "alter table " + tableName + " drop column (c2)");
        assertThat(columnVisibleInMetadata(connection, config.db().user(), tableName, "C2")).isFalse();
    }

    @Test
    @DisplayName("DEFECT-JDBC-015 PreparedStatement date round-trip preserves the calendar date")
    void preparedStatementDateRoundTripPreservesCalendarDate() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEF_JDBC_DATE_RT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key, d1 date)");

        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setDate(2, Date.valueOf("2024-02-29"));
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select d1 from " + tableName + " where id = 1")) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getDate(1)).isEqualTo(Date.valueOf("2024-02-29"));
            assertThat(resultSet.next()).isFalse();
        }
    }

    private JoinFixture createJoinFixture(String prefix) {
        String parentTable = DbTestSupport.uniqueName(prefix + "_P");
        String childTable = DbTestSupport.uniqueName(prefix + "_C");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, childTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, parentTable));

        jdbc.executeUpdate(connection, "create table " + parentTable + "(id integer primary key, note varchar(20))");
        jdbc.executeUpdate(connection, "create table " + childTable + "(id integer primary key, parent_id integer, note varchar(20))");
        for (int id = 1; id <= 5; id++) {
            jdbc.executeUpdate(connection, "insert into " + parentTable + " values(" + id + ", 'P" + id + "')");
        }
        jdbc.executeUpdate(connection, "insert into " + childTable + " values(10, 1, 'A')");
        jdbc.executeUpdate(connection, "insert into " + childTable + " values(11, 1, 'B')");
        jdbc.executeUpdate(connection, "insert into " + childTable + " values(12, 2, 'C')");
        jdbc.executeUpdate(connection, "insert into " + childTable + " values(13, null, 'NULL_PARENT')");
        jdbc.executeUpdate(connection, "insert into " + childTable + " values(14, 5, 'D')");
        return new JoinFixture(parentTable, childTable);
    }

    private String createAggregateFixture(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(grp integer, val integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 20)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, null)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 30)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 40)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, null)");
        return tableName;
    }

    private String createFuzzFixture(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection,
                "create table " + tableName + "(id integer primary key, c_num integer, c_text varchar(20), flag char(1))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 1, 'A', 'Y')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 2, 'B', 'N')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 2, 'B', 'Y')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(4, 4, 'SKIP', 'N')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(5, 5, 'C', 'Y')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(6, null, 'NULL_NUM', 'N')");
        return tableName;
    }

    private TypeMismatchFixture createTypeMismatchFixture(String prefix) {
        String numericTable = DbTestSupport.uniqueName(prefix + "_N");
        String textTable = DbTestSupport.uniqueName(prefix + "_T");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, textTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, numericTable));

        jdbc.executeUpdate(connection, "create table " + numericTable + "(id integer primary key, c_num integer)");
        jdbc.executeUpdate(connection, "create table " + textTable + "(c_text varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + numericTable + " values(1, 2)");
        jdbc.executeUpdate(connection, "insert into " + numericTable + " values(2, 10)");
        jdbc.executeUpdate(connection, "insert into " + numericTable + " values(3, 11)");
        jdbc.executeUpdate(connection, "insert into " + textTable + " values('2')");
        jdbc.executeUpdate(connection, "insert into " + textTable + " values('10')");
        return new TypeMismatchFixture(numericTable, textTable);
    }

    private TypeMismatchFixture createNumericAntiJoinFixture(String prefix) {
        String numericTable = DbTestSupport.uniqueName(prefix + "_N");
        String textTable = DbTestSupport.uniqueName(prefix + "_T");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, textTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, numericTable));

        jdbc.executeUpdate(connection, "create table " + numericTable + "(id integer primary key, c_num integer)");
        jdbc.executeUpdate(connection, "create table " + textTable + "(c_text varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + numericTable + " values(1, 1)");
        jdbc.executeUpdate(connection, "insert into " + numericTable + " values(2, 2)");
        jdbc.executeUpdate(connection, "insert into " + numericTable + " values(3, 3)");
        jdbc.executeUpdate(connection, "insert into " + textTable + " values('2')");
        jdbc.executeUpdate(connection, "insert into " + textTable + " values(null)");
        return new TypeMismatchFixture(numericTable, textTable);
    }

    private String createGroupByAliasFixture(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(grp integer, val integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 20)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 10)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 30)");
        return tableName;
    }

    private String createWindowEdgeFixture(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection,
                "create table " + tableName + "(id integer primary key, grp char(1), seq integer, val integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 'A', 1, 40)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2, 'A', 2, 60)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(3, 'B', 1, 50)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(4, 'B', 2, 20)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(5, 'C', 1, 70)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(6, 'C', 2, null)");
        return tableName;
    }

    private String createReplicatedTable(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));
        jdbc.executeUpdate(connection, "create table " + tableName + "(id integer primary key)");
        return tableName;
    }

    private void createPrivateDatabaseLink(String linkName) {
        jdbc.executeUpdate(
                connection,
                "create private database link " + linkName +
                        " connect to " + config.databaseLink().remoteUser() +
                        " identified by " + config.databaseLink().remotePassword() +
                        " using " + config.databaseLink().targetName()
        );
    }

    private void remoteExecuteImmediate(String linkName, String remoteSql) {
        try (CallableStatement callableStatement = connection.prepareCall("{call remote_execute_immediate(?, ?)}")) {
            callableStatement.setString(1, linkName);
            callableStatement.setString(2, remoteSql);
            callableStatement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("REMOTE_EXECUTE_IMMEDIATE failed: " + remoteSql, e);
        }
    }

    private String remoteTableSql(String linkName, String remoteSql) {
        return "select * from remote_table(" + linkName + ", '" + remoteSql.replace("'", "''") + "')";
    }

    private int resultRowCount(Statement statement, String sql) throws SQLException {
        int count = 0;
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                count++;
            }
        }
        return count;
    }

    private boolean tableVisibleInMetadata(String tableName) throws SQLException {
        return tableVisibleInMetadata(connection, config.db().user(), tableName);
    }

    private boolean tableVisibleInMetadata(Connection target, String schemaName, String tableName) throws SQLException {
        try (ResultSet resultSet = target.getMetaData().getTables(
                null,
                schemaName.toUpperCase(),
                tableName.toUpperCase(),
                null
        )) {
            return resultSet.next();
        }
    }

    private boolean columnVisibleInMetadata(
            Connection target,
            String schemaName,
            String tableName,
            String columnName
    ) throws SQLException {
        try (ResultSet resultSet = target.getMetaData().getColumns(
                null,
                schemaName.toUpperCase(),
                tableName.toUpperCase(),
                columnName.toUpperCase()
        )) {
            return resultSet.next();
        }
    }

    private boolean commitOrSerializationFailure(Connection target) {
        try {
            target.commit();
            target.setAutoCommit(true);
            return true;
        } catch (SQLException ignored) {
            rollbackQuietly(target);
            return false;
        }
    }

    private TimedUpdateResult executeUpdateOrConflict(Connection target, String sql) {
        long startedAt = System.nanoTime();
        try (Statement statement = target.createStatement()) {
            statement.setQueryTimeout(3);
            statement.executeUpdate(sql);
            return new TimedUpdateResult(true, elapsedMillisSince(startedAt));
        } catch (SQLException ignored) {
            rollbackQuietly(target);
            return new TimedUpdateResult(false, elapsedMillisSince(startedAt));
        }
    }

    private long elapsedMillisSince(long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000L;
    }

    private void gatherTableStats(Connection target, String schema, String objectName) {
        try (CallableStatement callableStatement = target.prepareCall("{call gather_table_stats(?, ?)}")) {
            callableStatement.setString(1, schema);
            callableStatement.setString(2, objectName);
            callableStatement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to gather table statistics for " + schema + "." + objectName, e);
        }
    }

    private TableStats getTableStats(Connection target, String schema, String objectName) {
        try (CallableStatement callableStatement = target.prepareCall("{call get_table_stats(?,?,?, ?,?,?,?,?)}")) {
            callableStatement.setString(1, schema);
            callableStatement.setString(2, objectName);
            callableStatement.setNull(3, Types.VARCHAR);
            callableStatement.registerOutParameter(4, Types.NUMERIC);
            callableStatement.registerOutParameter(5, Types.NUMERIC);
            callableStatement.registerOutParameter(6, Types.NUMERIC);
            callableStatement.registerOutParameter(7, Types.NUMERIC);
            callableStatement.registerOutParameter(8, Types.DOUBLE);
            callableStatement.execute();
            return new TableStats(callableStatement.getLong(4));
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query table statistics for " + schema + "." + objectName, e);
        }
    }

    private boolean indexExists(String indexName) {
        return jdbc.exists(connection,
                "select index_name from system_.sys_indices_ where index_name = '" + indexName.toUpperCase() + "'");
    }

    private int[] callTcpEchoProcedure(String prefix, String payload) throws SQLException {
        String procedureName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        jdbc.executeUpdate(connection,
                "create or replace procedure " + procedureName +
                        "(p_connected out integer, p_written out integer, p_closed out integer) as " +
                        "c connect_type; " +
                        "begin " +
                        "c := open_connect('" + config.network().tcpHost() + "', " + config.network().tcpPort() + ", 1000, 3000); " +
                        "p_connected := is_connected(c); " +
                        "p_written := write_raw(c, to_raw('" + payload + "' || chr(10)), raw_sizeof('" + payload + "' || chr(10))); " +
                        "p_closed := close_connect(c); " +
                        "end;");

        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(?,?,?)}")) {
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.execute();
            return new int[]{
                    callableStatement.getInt(1),
                    callableStatement.getInt(2),
                    callableStatement.getInt(3)
            };
        }
    }

    private int[] callTcpConnectionStateProcedure(String procedureName) throws SQLException {
        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(?,?)}")) {
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.execute();
            return new int[]{
                    callableStatement.getInt(1),
                    callableStatement.getInt(2)
            };
        }
    }

    private int[] callClosedTcpWriteProcedure(String procedureName) throws SQLException {
        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(?,?)}")) {
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.execute();
            return new int[]{
                    callableStatement.getInt(1),
                    callableStatement.getInt(2)
            };
        }
    }

    private String callSmtpSendProcedure(String prefix) throws SQLException {
        String procedureName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        jdbc.executeUpdate(connection,
                "create or replace procedure " + procedureName + "(p_response out varchar(65534)) as " +
                        "c connect_type; " +
                        "r varchar(65534); " +
                        "begin " +
                        "c := utl_smtp.open_connection('" + config.network().smtpHost() + "', " +
                        config.network().smtpPort() + ", null); " +
                        "r := utl_smtp.helo(c, 'localhost'); " +
                        "r := utl_smtp.mail(c, 'qa@example.com'); " +
                        "r := utl_smtp.rcpt(c, 'qa@example.com'); " +
                        "r := utl_smtp.open_data(c); " +
                        "utl_smtp.write_data(c, 'Subject: altibase qa' || chr(13) || chr(10) || chr(13) || chr(10) || 'hello'); " +
                        "r := utl_smtp.close_data(c); " +
                        "p_response := utl_smtp.quit(c); " +
                        "end;");

        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(?)}")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.execute();
            return callableStatement.getString(1);
        }
    }

    private void createDirectoryRoundTripProcedure(String procedureName, String directoryName, String fileName) {
        jdbc.executeUpdate(connection,
                "create or replace procedure " + procedureName + "(p_first out varchar(200), p_second out varchar(200)) as " +
                        "f FILE_TYPE; " +
                        "begin " +
                        "f := FOPEN('" + directoryName + "', '" + fileName + "', 'W'); " +
                        "PUT_LINE(f, 'FIRST_LINE'); " +
                        "PUT_LINE(f, 'SECOND_LINE'); " +
                        "FCLOSE(f); " +
                        "f := FOPEN('" + directoryName + "', '" + fileName + "', 'R'); " +
                        "GET_LINE(f, p_first); " +
                        "GET_LINE(f, p_second); " +
                        "FCLOSE(f); " +
                        "end;");
    }

    private String[] callDirectoryRoundTripProcedure(String procedureName) throws SQLException {
        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(?,?)}")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            callableStatement.execute();
            return new String[]{
                    stripLineTerminator(callableStatement.getString(1)),
                    stripLineTerminator(callableStatement.getString(2))
            };
        }
    }

    private String stripLineTerminator(String value) {
        return value == null ? null : value.replaceAll("[\\r\\n]+$", "");
    }

    private void assertProcedureFails(String procedureName) {
        assertThatThrownBy(() -> callNoArgProcedure(procedureName))
                .isInstanceOf(SQLException.class);
    }

    private void callNoArgProcedure(String procedureName) throws SQLException {
        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "()}")) {
            callableStatement.execute();
        }
    }

    private void dropDirectoryQuietly(String directoryName) {
        try {
            jdbc.executeUpdate(connection, "drop directory " + directoryName);
        } catch (Exception ignored) {
        }
    }

    private boolean isArchiveLogMode() {
        String archiveMode = jdbc.queryForString(connection, "select archive_mode from v$archive");
        return "1".equals(archiveMode);
    }

    private boolean tablespaceExists(String tablespaceName) {
        return jdbc.exists(connection,
                "select name from v$tablespaces where name = '" + tablespaceName.toUpperCase() + "'");
    }

    private void executeAsSysdba(String sql) {
        try (Connection sysdba = jdbc.openSysdba()) {
            jdbc.executeUpdate(sysdba, sql);
        } catch (Exception e) {
            throw new IllegalStateException("SYSDBA SQL failed: " + sql, e);
        }
    }

    private void assertSysdbaUpdateFails(String sql) {
        assertThatThrownBy(() -> executeAsSysdba(sql))
                .isInstanceOf(IllegalStateException.class)
                .satisfies(throwable -> assertThat((Object) SqlExceptionSupport.findSqlException(throwable)).isNotNull());
    }

    private void endBackupQuietly(String tablespaceName) {
        try {
            executeAsSysdba("alter tablespace " + tablespaceName + " end backup");
        } catch (Exception ignored) {
        }
    }

    private void grantIfNeeded(String privilege, String grantee) {
        try {
            jdbc.executeUpdate(connection, "grant " + privilege + " to " + grantee);
        } catch (IllegalStateException e) {
            if (isAlreadyGranted(e)) {
                return;
            }
            throw e;
        }
    }

    private boolean isAlreadyGranted(IllegalStateException e) {
        SQLException sqlException = SqlExceptionSupport.findSqlException(e);
        return sqlException != null
                && sqlException.getMessage() != null
                && sqlException.getMessage().contains("already has privileges");
    }

    private String selfHostedWithClause() {
        return "with '" + config.replication().remoteHost() + "', " + replicationPort();
    }

    private int replicationPort() {
        int configured = config.replication().remotePort();
        if (configured > 0) {
            return configured;
        }
        return Integer.parseInt(jdbc.queryForString(connection,
                "select value1 from v$property where name = 'REPLICATION_PORT_NO'").trim());
    }

    private String replicationTableClause(String... tableNames) {
        List<String> clauses = new ArrayList<>();
        for (String tableName : tableNames) {
            clauses.add(tableMappingClause(tableName));
        }
        return String.join(", ", clauses);
    }

    private String tableMappingClause(String tableName) {
        String schema = config.db().user().toLowerCase();
        return "from " + schema + "." + tableName + " to " + schema + "." + tableName;
    }

    private int replicationCount(String replicationName) {
        return Integer.parseInt(jdbc.queryForString(connection,
                "select count(*) from system_.sys_replications_ where replication_name = '" + replicationName + "'"));
    }

    private void assertReplicationCounts(String replicationName, int expectedItemCount, int expectedHostCount) {
        assertThat(replicationCount(replicationName)).isEqualTo(1);
        assertThat(jdbc.queryForString(connection,
                "select item_count from system_.sys_replications_ where replication_name = '" + replicationName + "'"))
                .isEqualTo(String.valueOf(expectedItemCount));
        assertThat(jdbc.queryForString(connection,
                "select host_count from system_.sys_replications_ where replication_name = '" + replicationName + "'"))
                .isEqualTo(String.valueOf(expectedHostCount));
    }

    private int replicationIsStarted(String replicationName) {
        return Integer.parseInt(jdbc.queryForString(connection,
                "select is_started from system_.sys_replications_ where replication_name = '" + replicationName + "'"));
    }

    private void dropReplicationQuietly(String replicationName) {
        try {
            jdbc.executeUpdate(connection, "drop replication " + replicationName);
        } catch (Exception ignored) {
        }
    }

    private void createDmlTarget(String owner, String tableName, int id, int value) throws Exception {
        withUserConnection(owner, conn -> {
            jdbc.executeUpdate(conn, "create table " + tableName + "(id integer primary key, val integer)");
            jdbc.executeUpdate(conn, "insert into " + tableName + " values(" + id + ", " + value + ")");
        });
    }

    private void insertDmlRow(String owner, String tableName, int id, int value) throws Exception {
        withUserConnection(owner, conn ->
                jdbc.executeUpdate(conn, "insert into " + tableName + " values(" + id + ", " + value + ")"));
    }

    private void grantTablePrivileges(String owner, String tableName, String grantee, String... privileges) {
        for (String privilege : privileges) {
            jdbc.executeUpdate(connection, "grant " + privilege + " on " + owner + "." + tableName + " to " + grantee);
        }
    }

    private int rowCount(Connection conn, String tableName, String predicate) {
        return Integer.parseInt(jdbc.queryForString(conn,
                "select count(*) from " + tableName + " where " + predicate));
    }

    private void setInts(PreparedStatement preparedStatement, Integer... values) throws SQLException {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                preparedStatement.setNull(i + 1, Types.INTEGER);
            } else {
                preparedStatement.setInt(i + 1, values[i]);
            }
        }
    }

    private void executeCachedUpdateOrAcceptFailure(PreparedStatement preparedStatement) throws SQLException {
        try {
            preparedStatement.executeUpdate();
        } catch (SQLException ignored) {
            assertThat(jdbc.queryForString(preparedStatement.getConnection(), "select count(*) from dual"))
                    .isEqualTo("1");
        }
    }

    private void assertCachedPreparedUpdateFails(PreparedStatement preparedStatement) throws SQLException {
        try {
            int updated = preparedStatement.executeUpdate();
            fail("Cached prepared update unexpectedly succeeded with update count <%s>", updated);
        } catch (SQLException ignored) {
            assertThat(jdbc.queryForString(preparedStatement.getConnection(), "select count(*) from dual"))
                    .isEqualTo("1");
        }
    }

    private void assertCachedPreparedUpdateSucceeds(PreparedStatement preparedStatement) throws SQLException {
        try {
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        } catch (SQLException e) {
            fail("Cached prepared update should have used the current object definition", e);
        }
    }

    private void createBeforeInsertValueTrigger(String triggerName, String tableName, int value) {
        jdbc.executeUpdate(connection,
                "create or replace trigger " + triggerName + " before insert on " + tableName +
                        " referencing new row new_row for each row as begin " +
                        "if new_row.val is null then new_row.val := " + value + "; end if; end;");
    }

    private void createBeforeInsertValueTriggerDisabled(String triggerName, String tableName, int value) {
        jdbc.executeUpdate(connection,
                "create or replace trigger " + triggerName + " before insert on " + tableName +
                        " referencing new row new_row for each row disable as begin " +
                        "if new_row.val is null then new_row.val := " + value + "; end if; end;");
    }

    private void dropSequenceQuietly(String sequenceName) {
        try {
            jdbc.executeUpdate(connection, "drop sequence " + sequenceName);
        } catch (Exception ignored) {
        }
    }

    private String createManagedUser(String prefix) {
        String user = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropUserQuietly(jdbc, connection, user));
        jdbc.executeUpdate(connection, "create user " + user + " identified by " + user);
        return user;
    }

    private String createManagedRole(String prefix) {
        String role = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropRoleQuietly(jdbc, connection, role));
        jdbc.executeUpdate(connection, "create role " + role);
        return role;
    }

    private void createSimpleOutProcedure(Connection conn, String procedureName) {
        jdbc.executeUpdate(conn, "create or replace procedure " + procedureName +
                "(p1 out integer) as begin p1 := 1; end;");
    }

    private void createOutProcedureReturning(Connection conn, String procedureName, int value) {
        jdbc.executeUpdate(conn, "create or replace procedure " + procedureName +
                "(p1 out integer) as begin p1 := " + value + "; end;");
    }

    private void createSimpleFunction(Connection conn, String functionName) {
        jdbc.executeUpdate(conn, "create or replace function " + functionName +
                "(p1 in integer) return integer as begin return p1 * 2; end;");
    }

    private void createFunctionMultiplier(Connection conn, String functionName, int multiplier) {
        jdbc.executeUpdate(conn, "create or replace function " + functionName +
                "(p1 in integer) return integer as begin return p1 * " + multiplier + "; end;");
    }

    private int executeOutProcedure(Connection conn, String qualifiedProcedureName) throws SQLException {
        try (CallableStatement callableStatement = conn.prepareCall("{call " + qualifiedProcedureName + "(?)}")) {
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.execute();
            return callableStatement.getInt(1);
        }
    }

    private void executeNoArgProcedure(Connection conn, String qualifiedProcedureName) throws SQLException {
        try (CallableStatement callableStatement = conn.prepareCall("{call " + qualifiedProcedureName + "()}")) {
            callableStatement.execute();
        }
    }

    private int executeFunction(Connection conn, String qualifiedFunctionName, int value) throws SQLException {
        try (CallableStatement callableStatement = conn.prepareCall("{ ? = call " + qualifiedFunctionName + "(?) }")) {
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.setInt(2, value);
            callableStatement.execute();
            return callableStatement.getInt(1);
        }
    }

    private void assertCachedPreparedStatementYieldsFreshOrFails(
            PreparedStatement preparedStatement,
            String freshValue
    ) throws SQLException {
        try {
            assertThat(singleString(preparedStatement)).isEqualTo(freshValue);
        } catch (SQLException ignored) {
            assertThat(jdbc.queryForString(preparedStatement.getConnection(), "select count(*) from dual"))
                    .isEqualTo("1");
        }
    }

    private void assertCachedPreparedStatementFails(PreparedStatement preparedStatement) throws SQLException {
        try {
            String value = singleString(preparedStatement);
            fail("Cached prepared statement unexpectedly succeeded with value <%s>", value);
        } catch (SQLException ignored) {
            assertThat(jdbc.queryForString(preparedStatement.getConnection(), "select count(*) from dual"))
                    .isEqualTo("1");
        }
    }

    private void assertCachedOutProcedureYieldsFreshOrFails(
            CallableStatement callableStatement,
            int freshValue
    ) throws SQLException {
        try {
            callableStatement.execute();
            assertThat(callableStatement.getInt(1)).isEqualTo(freshValue);
        } catch (SQLException ignored) {
            assertThat(jdbc.queryForString(callableStatement.getConnection(), "select count(*) from dual"))
                    .isEqualTo("1");
        }
    }

    private void assertCachedOutProcedureFails(CallableStatement callableStatement) throws SQLException {
        try {
            callableStatement.execute();
            fail("Cached callable unexpectedly succeeded with value <%s>", callableStatement.getInt(1));
        } catch (SQLException ignored) {
            assertThat(jdbc.queryForString(callableStatement.getConnection(), "select count(*) from dual"))
                    .isEqualTo("1");
        }
    }

    private void assertCachedFunctionYieldsFreshOrFails(
            CallableStatement callableStatement,
            int argument,
            int freshValue
    ) throws SQLException {
        try {
            callableStatement.setInt(2, argument);
            callableStatement.execute();
            assertThat(callableStatement.getInt(1)).isEqualTo(freshValue);
        } catch (SQLException ignored) {
            assertThat(jdbc.queryForString(callableStatement.getConnection(), "select count(*) from dual"))
                    .isEqualTo("1");
        }
    }

    private void withUserConnection(String user, SqlWork work) throws Exception {
        try (Connection userConnection = jdbc.open(user, user)) {
            work.run(userConnection);
        }
    }

    private String singleString(PreparedStatement preparedStatement) throws SQLException {
        try (var resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next()).isTrue();
            Object value = resultSet.getObject(1);
            return value == null ? null : value.toString();
        }
    }

    private void assertSameRows(String leftSql, String rightSql) {
        List<String> leftRows = canonicalRows(leftSql);
        List<String> rightRows = canonicalRows(rightSql);
        assertThat(leftRows)
                .as("left SQL:%n%s%nright SQL:%n%s", leftSql, rightSql)
                .isEqualTo(rightRows);
    }

    private List<String> canonicalRows(String sql) {
        List<String> rows = orderedRows(sql);
        rows.sort(String::compareTo);
        return rows;
    }

    private List<String> orderedRows(String sql) {
        QueryResult result = jdbc.query(connection, sql);
        List<String> rows = new ArrayList<>();
        for (Map<String, Object> row : result.rows()) {
            rows.add(result.columns().stream()
                    .map(column -> normalize(row.get(column)))
                    .collect(Collectors.joining("|")));
        }
        return rows;
    }

    private String normalize(Object value) {
        return value == null ? "<NULL>" : value.toString();
    }

    private void rollbackQuietly(Connection target) {
        if (target == null) {
            return;
        }
        try {
            if (!target.getAutoCommit()) {
                target.rollback();
                target.setAutoCommit(true);
            }
        } catch (Exception ignored) {
        }
    }

    private record JoinFixture(String parentTable, String childTable) {
    }

    private record TypeMismatchFixture(String numericTable, String textTable) {
    }

    private record TableStats(long numRows) {
    }

    private record TimedUpdateResult(boolean succeeded, long elapsedMillis) {
        double elapsedSeconds() {
            return elapsedMillis / 1000.0;
        }
    }

    @FunctionalInterface
    private interface SqlWork {
        void run(Connection connection) throws Exception;
    }
}
