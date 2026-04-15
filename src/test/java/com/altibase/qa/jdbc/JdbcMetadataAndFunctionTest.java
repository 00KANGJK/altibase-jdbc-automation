package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class JdbcMetadataAndFunctionTest extends BaseDbTest {

    @Test
    @DisplayName("JDBC connection metadata exposes database product information")
    void jdbcConnectionMetadata() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        assertThat(metaData.getDatabaseProductName()).containsIgnoringCase("altibase");
        assertThat(metaData.getDatabaseProductVersion()).isNotBlank();
    }

    @Test
    @DisplayName("TC_126_001 RANK window function assigns ranking within partitions")
    void tc126001Rank() {
        String tableName = DbTestSupport.uniqueName("QA_FUNC_RANK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(e_lastname varchar(20), dno int, salary int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('KIM', 10, 1000)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('LEE', 10, 800)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('PARK', 20, 900)");

        QueryResult result = jdbc.query(
                connection,
                "select e_lastname, dno, salary, rank() over (partition by dno order by salary desc) as rnk " +
                        "from " + tableName + " order by dno, salary desc, e_lastname"
        );

        assertThat(result.size()).isEqualTo(3);
        assertThat(((Number) result.value(0, "RNK")).intValue()).isEqualTo(1);
        assertThat(((Number) result.value(1, "RNK")).intValue()).isEqualTo(2);
        assertThat(((Number) result.value(2, "RNK")).intValue()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_126_002 LAG window function returns prior row values with offsets")
    void tc126002Lag() {
        String tableName = createAnalyticSampleTable("QA_FUNC_LAG");

        QueryResult result = jdbc.query(
                connection,
                "select salary, lag(salary, 2, 0) over (order by salary) as lag_value " +
                        "from " + tableName + " order by salary"
        );

        assertThat(((Number) result.value(0, "LAG_VALUE")).intValue()).isEqualTo(0);
        assertThat(((Number) result.value(1, "LAG_VALUE")).intValue()).isEqualTo(0);
        assertThat(((Number) result.value(2, "LAG_VALUE")).intValue()).isEqualTo(100);
        assertThat(((Number) result.value(5, "LAG_VALUE")).intValue()).isEqualTo(250);
    }

    @Test
    @DisplayName("TC_126_003 NTILE window function assigns bucket numbers")
    void tc126003Ntile() {
        String tableName = createAnalyticSampleTable("QA_FUNC_NTILE");

        QueryResult result = jdbc.query(
                connection,
                "select salary, ntile(3) over (order by salary) as bucket " +
                        "from " + tableName + " order by salary"
        );

        assertThat(((Number) result.value(0, "BUCKET")).intValue()).isEqualTo(1);
        assertThat(((Number) result.value(1, "BUCKET")).intValue()).isEqualTo(1);
        assertThat(((Number) result.value(2, "BUCKET")).intValue()).isEqualTo(2);
        assertThat(((Number) result.value(5, "BUCKET")).intValue()).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_127_001 FIRST_VALUE window function returns the first value in a partition")
    void tc127001FirstValue() {
        String tableName = DbTestSupport.uniqueName("QA_FUNC_FVAL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(sex char(1), salary int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('M', 300)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('M', 100)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('F', 200)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('F', 150)");

        QueryResult result = jdbc.query(
                connection,
                "select sex, salary, first_value(salary) over (" +
                        "partition by sex order by salary rows between unbounded preceding and unbounded following" +
                        ") as f_value from " + tableName + " order by sex, salary"
        );

        assertThat(((Number) result.value(0, "F_VALUE")).intValue()).isEqualTo(150);
        assertThat(((Number) result.value(1, "F_VALUE")).intValue()).isEqualTo(150);
        assertThat(((Number) result.value(2, "F_VALUE")).intValue()).isEqualTo(100);
        assertThat(((Number) result.value(3, "F_VALUE")).intValue()).isEqualTo(100);
    }

    @Test
    @DisplayName("TC_127_002 LAST_VALUE window function returns the last value in a partition")
    void tc127002LastValue() {
        String tableName = createAnalyticSampleTable("QA_FUNC_LASTVAL");

        QueryResult result = jdbc.query(
                connection,
                "select sex, salary, last_value(salary) over (" +
                        "partition by sex order by salary rows between unbounded preceding and unbounded following" +
                        ") as last_value_result from " + tableName + " order by sex, salary"
        );

        assertThat(((Number) result.value(0, "LAST_VALUE_RESULT")).intValue()).isEqualTo(350);
        assertThat(((Number) result.value(2, "LAST_VALUE_RESULT")).intValue()).isEqualTo(350);
        assertThat(((Number) result.value(3, "LAST_VALUE_RESULT")).intValue()).isEqualTo(300);
        assertThat(((Number) result.value(5, "LAST_VALUE_RESULT")).intValue()).isEqualTo(300);
    }

    @Test
    @DisplayName("TC_127_003 NTH_VALUE window function returns the nth value in a partition")
    void tc127003NthValue() {
        String tableName = createAnalyticSampleTable("QA_FUNC_NTHVAL");

        QueryResult result = jdbc.query(
                connection,
                "select sex, salary, nth_value(salary, 2) over (" +
                        "partition by sex order by salary rows between unbounded preceding and unbounded following" +
                        ") as nth_value_result from " + tableName + " order by sex, salary"
        );

        assertThat(((Number) result.value(0, "NTH_VALUE_RESULT")).intValue()).isEqualTo(250);
        assertThat(((Number) result.value(2, "NTH_VALUE_RESULT")).intValue()).isEqualTo(250);
        assertThat(((Number) result.value(3, "NTH_VALUE_RESULT")).intValue()).isEqualTo(200);
        assertThat(((Number) result.value(5, "NTH_VALUE_RESULT")).intValue()).isEqualTo(200);
    }

    @Test
    @DisplayName("TC_103_001 AVG function returns average")
    void tc103001Avg() {
        String value = jdbc.queryForString(connection, "select avg(value) as avg_value from (select 10 as value from dual union all select 20 from dual union all select 30 from dual) a");
        assertThat(value).isEqualTo("20");
    }

    @Test
    @DisplayName("TC_104_001 CORR function returns correlation")
    void tc104001Corr() {
        String tableName = createAnalyticSampleTable("QA_FUNC_CORR_DIRECT");

        QueryResult result = jdbc.query(connection, "select corr(eno, salary) as corr_value from " + tableName);
        assertThat(((Number) result.value(0, "CORR_VALUE")).doubleValue()).isCloseTo(0.7142857142857143d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_105_001 COUNT function returns row count")
    void tc105001Count() {
        String value = jdbc.queryForString(connection, "select count(*) as cnt from (select 1 from dual union all select 1 from dual union all select 1 from dual) a");
        assertThat(value).isEqualTo("3");
    }

    @Test
    @DisplayName("TC_106_001 COVAR_POP function returns population covariance")
    void tc106001CovarPop() {
        String tableName = createAnalyticSampleTable("QA_FUNC_COVAR_POP_DIRECT");

        QueryResult result = jdbc.query(connection, "select covar_pop(eno, salary) as covar_pop_value from " + tableName);
        assertThat(((Number) result.value(0, "COVAR_POP_VALUE")).doubleValue()).isCloseTo(104.16666666666667d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_107_001 COVAR_SAMP function returns sample covariance")
    void tc107001CovarSamp() {
        String tableName = createAnalyticSampleTable("QA_FUNC_COVAR_SAMP_DIRECT");

        QueryResult result = jdbc.query(connection, "select covar_samp(eno, salary) as covar_samp_value from " + tableName);
        assertThat(((Number) result.value(0, "COVAR_SAMP_VALUE")).doubleValue()).isEqualTo(125d);
    }

    @Test
    @DisplayName("TC_108_001 CUME_DIST within-group function returns cumulative distribution")
    void tc108001CumeDist() {
        String tableName = createOrderedSetSampleTable("QA_FUNC_CUME_DIST");

        QueryResult result = jdbc.query(
                connection,
                "select cume_dist(1500) within group (order by salary) as cume_dist_value from " + tableName
        );

        assertThat(((Number) result.value(0, "CUME_DIST_VALUE")).doubleValue()).isCloseTo(0.5d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_109_001 KEEP(DENSE_RANK FIRST) returns the first-ranked aggregate value")
    void tc109001KeepDenseRankFirst() {
        String tableName = createKeepDenseRankSampleTable("QA_FUNC_KEEP_FIRST");

        QueryResult result = jdbc.query(
                connection,
                "select dno, max(eno) keep (dense_rank first order by salary desc) as empno " +
                        "from " + tableName + " group by dno order by dno"
        );

        assertThat(((Number) result.value(0, "EMPNO")).intValue()).isEqualTo(1);
        assertThat(((Number) result.value(1, "EMPNO")).intValue()).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_111_001 KEEP(DENSE_RANK LAST) returns the last-ranked aggregate value")
    void tc111001KeepDenseRankLast() {
        String tableName = createKeepDenseRankSampleTable("QA_FUNC_KEEP_LAST");

        QueryResult result = jdbc.query(
                connection,
                "select dno, min(eno) keep (dense_rank last order by salary desc) as empno " +
                        "from " + tableName + " group by dno order by dno"
        );

        assertThat(((Number) result.value(0, "EMPNO")).intValue()).isEqualTo(2);
        assertThat(((Number) result.value(1, "EMPNO")).intValue()).isEqualTo(4);
    }

    @Test
    @DisplayName("TC_112_001 MAX function returns maximum")
    void tc112001Max() {
        String value = jdbc.queryForString(connection, "select max(value) as max_value from (select 10 as value from dual union all select 30 from dual union all select 20 from dual) a");
        assertThat(value).isEqualTo("30");
    }

    @Test
    @DisplayName("TC_113_001 MIN function returns minimum")
    void tc113001Min() {
        String value = jdbc.queryForString(connection, "select min(value) as min_value from (select 10 as value from dual union all select 30 from dual union all select 20 from dual) a");
        assertThat(value).isEqualTo("10");
    }

    @Test
    @DisplayName("TC_114_001 PERCENTILE_CONT returns continuous percentile values")
    void tc114001PercentileCont() {
        String tableName = createAnalyticSampleTable("QA_FUNC_PCONT_DIRECT");

        QueryResult result = jdbc.query(connection, "select percentile_cont(0.5) within group (order by salary asc) as percentile_cont_value from " + tableName);
        assertThat(((Number) result.value(0, "PERCENTILE_CONT_VALUE")).doubleValue()).isEqualTo(225d);
    }

    @Test
    @DisplayName("TC_115_001 PERCENTILE_DISC returns discrete percentile values")
    void tc115001PercentileDisc() {
        String tableName = createAnalyticSampleTable("QA_FUNC_PDISC_DIRECT");

        QueryResult result = jdbc.query(connection, "select percentile_disc(0.5) within group (order by salary asc) as percentile_disc_value from " + tableName);
        assertThat(((Number) result.value(0, "PERCENTILE_DISC_VALUE")).intValue()).isEqualTo(200);
    }

    @Test
    @DisplayName("TC_116_001 PERCENT_RANK within-group function returns percentile rank")
    void tc116001PercentRank() {
        String tableName = createOrderedSetSampleTable("QA_FUNC_PERCENT_RANK");

        QueryResult result = jdbc.query(
                connection,
                "select percent_rank(1003, 1000) within group (order by dno, salary) as percent_rank_value from " + tableName
        );

        assertThat(((Number) result.value(0, "PERCENT_RANK_VALUE")).doubleValue()).isCloseTo(0.2d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_117_001 RANK within-group function returns the rank of a hypothetical row")
    void tc117001RankWithinGroup() {
        String tableName = createOrderedSetSampleTable("QA_FUNC_RANK_WITHIN");

        QueryResult result = jdbc.query(
                connection,
                "select rank(1003, 1001) within group (order by dno, salary) as rank_value from " + tableName
        );

        assertThat(((Number) result.value(0, "RANK_VALUE")).intValue()).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_118_001 STATS_ONE_WAY_ANOVA returns SUM_SQUARES_BETWEEN")
    void tc118001StatsOneWayAnovaSumSquaresBetween() {
        String tableName = createAnovaSampleTable("QA_FUNC_ANOVA_SSB");

        QueryResult result = jdbc.query(
                connection,
                "select stats_one_way_anova(group_id, amount, 'SUM_SQUARES_BETWEEN') as anova_value from " + tableName
        );

        assertThat(((Number) result.value(0, "ANOVA_VALUE")).doubleValue()).isCloseTo(36d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_118_002 STATS_ONE_WAY_ANOVA returns SUM_SQUARES_WITHIN")
    void tc118002StatsOneWayAnovaSumSquaresWithin() {
        String tableName = createAnovaSampleTable("QA_FUNC_ANOVA_SSW");

        QueryResult result = jdbc.query(
                connection,
                "select stats_one_way_anova(group_id, amount, 'SUM_SQUARES_WITHIN') as anova_value from " + tableName
        );

        assertThat(((Number) result.value(0, "ANOVA_VALUE")).doubleValue()).isCloseTo(1.5d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_118_003 STATS_ONE_WAY_ANOVA returns MEAN_SQUARES_BETWEEN")
    void tc118003StatsOneWayAnovaMeanSquaresBetween() {
        String tableName = createAnovaSampleTable("QA_FUNC_ANOVA_MSB");

        QueryResult result = jdbc.query(
                connection,
                "select stats_one_way_anova(group_id, amount, 'MEAN_SQUARES_BETWEEN') as anova_value from " + tableName
        );

        assertThat(((Number) result.value(0, "ANOVA_VALUE")).doubleValue()).isCloseTo(18d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_118_004 STATS_ONE_WAY_ANOVA returns DF_WITHIN")
    void tc118004StatsOneWayAnovaDfWithin() {
        String tableName = createAnovaSampleTable("QA_FUNC_ANOVA_DFW");

        QueryResult result = jdbc.query(
                connection,
                "select stats_one_way_anova(group_id, amount, 'DF_WITHIN') as anova_value from " + tableName
        );

        assertThat(((Number) result.value(0, "ANOVA_VALUE")).doubleValue()).isCloseTo(3d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_118_005 STATS_ONE_WAY_ANOVA returns DF_BETWEEN")
    void tc118005StatsOneWayAnovaDfBetween() {
        String tableName = createAnovaSampleTable("QA_FUNC_ANOVA_DFB");

        QueryResult result = jdbc.query(
                connection,
                "select stats_one_way_anova(group_id, amount, 'DF_BETWEEN') as anova_value from " + tableName
        );

        assertThat(((Number) result.value(0, "ANOVA_VALUE")).doubleValue()).isCloseTo(2d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_118_006 STATS_ONE_WAY_ANOVA returns the within-group sum of squares")
    void tc118006StatsOneWayAnovaWithinGroupSumSquares() {
        String tableName = createAnovaSampleTable("QA_FUNC_ANOVA_SSW_DUP");

        QueryResult result = jdbc.query(
                connection,
                "select stats_one_way_anova(group_id, amount, 'SUM_SQUARES_WITHIN') as anova_value from " + tableName
        );

        assertThat(((Number) result.value(0, "ANOVA_VALUE")).doubleValue()).isCloseTo(1.5d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_118_007 STATS_ONE_WAY_ANOVA returns the between-group sum of squares")
    void tc118007StatsOneWayAnovaBetweenGroupSumSquares() {
        String tableName = createAnovaSampleTable("QA_FUNC_ANOVA_SSB_DUP");

        QueryResult result = jdbc.query(
                connection,
                "select stats_one_way_anova(group_id, amount, 'SUM_SQUARES_BETWEEN') as anova_value from " + tableName
        );

        assertThat(((Number) result.value(0, "ANOVA_VALUE")).doubleValue()).isCloseTo(36d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_122_001 SUM function returns total")
    void tc122001Sum() {
        String sum = jdbc.queryForString(connection, "select sum(value) as total from (select 10 as value from dual union all select 20 from dual) a");
        assertThat(sum).isEqualTo("30");
    }

    @Test
    @DisplayName("TC_121_001 STDDEV_SAMP function returns sample standard deviation")
    void tc121001StddevSamp() {
        String tableName = createAnalyticSampleTable("QA_FUNC_STDDEV_SAMP");

        QueryResult result = jdbc.query(connection, "select stddev_samp(salary) as stddev_samp_value from " + tableName);
        assertThat(((Number) result.value(0, "STDDEV_SAMP_VALUE")).doubleValue()).isCloseTo(93.54143d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_119_001 STDDEV function returns standard deviation")
    void tc119001Stddev() {
        String tableName = createAnalyticSampleTable("QA_FUNC_STDDEV_DIRECT");

        QueryResult result = jdbc.query(connection, "select stddev(salary) as stddev_value from " + tableName);
        assertThat(((Number) result.value(0, "STDDEV_VALUE")).doubleValue()).isCloseTo(93.54143d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_120_001 STDDEV_POP function returns population standard deviation")
    void tc120001StddevPop() {
        String tableName = createAnalyticSampleTable("QA_FUNC_STDDEV_POP");

        QueryResult result = jdbc.query(connection, "select stddev_pop(salary) as stddev_pop_value from " + tableName);
        assertThat(((Number) result.value(0, "STDDEV_POP_VALUE")).doubleValue()).isCloseTo(85.39125638d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_123_001 VARIANCE function returns variance")
    void tc123001Variance() {
        String tableName = createAnalyticSampleTable("QA_FUNC_VARIANCE");

        QueryResult result = jdbc.query(connection, "select variance(salary) as variance_value from " + tableName);
        assertThat(((Number) result.value(0, "VARIANCE_VALUE")).doubleValue()).isEqualTo(8750d);
    }

    @Test
    @DisplayName("TC_124_001 VAR_POP function returns population variance")
    void tc124001VarPop() {
        String tableName = createAnalyticSampleTable("QA_FUNC_VAR_POP");

        QueryResult result = jdbc.query(connection, "select var_pop(salary) as var_pop_value from " + tableName);
        assertThat(((Number) result.value(0, "VAR_POP_VALUE")).doubleValue()).isCloseTo(7291.66667d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_125_001 AVG window testcase returns average")
    void tc125001AvgWindowCase() {
        String tableName = createAnalyticSampleTable("QA_FUNC_AVG_WIN");

        QueryResult result = jdbc.query(connection, "select avg(salary) as avg_salary from " + tableName);
        assertThat(((Number) result.value(0, "AVG_SALARY")).doubleValue()).isEqualTo(225d);
    }

    @Test
    @DisplayName("TC_125_002 CORR function returns correlation")
    void tc125002Corr() {
        String tableName = createAnalyticSampleTable("QA_FUNC_CORR");

        QueryResult result = jdbc.query(connection, "select corr(eno, salary) as corr_value from " + tableName);
        assertThat(((Number) result.value(0, "CORR_VALUE")).doubleValue()).isCloseTo(0.7142857142857143d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_125_003 COUNT window testcase returns row count")
    void tc125003CountWindowCase() {
        String tableName = createAnalyticSampleTable("QA_FUNC_COUNT_WIN");

        QueryResult result = jdbc.query(connection, "select count(*) as rec_count from " + tableName);
        assertThat(((Number) result.value(0, "REC_COUNT")).intValue()).isEqualTo(6);
    }

    @Test
    @DisplayName("TC_125_004 COVAR_POP function returns population covariance")
    void tc125004CovarPop() {
        String tableName = createAnalyticSampleTable("QA_FUNC_COVAR_POP");

        QueryResult result = jdbc.query(connection, "select covar_pop(eno, salary) as covar_pop_value from " + tableName);
        assertThat(((Number) result.value(0, "COVAR_POP_VALUE")).doubleValue()).isCloseTo(104.16666666666667d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_125_005 COVAR_SAMP function returns sample covariance")
    void tc125005CovarSamp() {
        String tableName = createAnalyticSampleTable("QA_FUNC_COVAR_SAMP");

        QueryResult result = jdbc.query(connection, "select covar_samp(eno, salary) as covar_samp_value from " + tableName);
        assertThat(((Number) result.value(0, "COVAR_SAMP_VALUE")).doubleValue()).isEqualTo(125d);
    }

    @Test
    @DisplayName("TC_125_006 MAX window testcase returns maximum")
    void tc125006MaxWindowCase() {
        String tableName = createAnalyticSampleTable("QA_FUNC_MAX_WIN");

        QueryResult result = jdbc.query(connection, "select max(salary) as max_salary from " + tableName);
        assertThat(((Number) result.value(0, "MAX_SALARY")).intValue()).isEqualTo(350);
    }

    @Test
    @DisplayName("TC_125_007 MIN window testcase returns minimum")
    void tc125007MinWindowCase() {
        String tableName = createAnalyticSampleTable("QA_FUNC_MIN_WIN");

        QueryResult result = jdbc.query(connection, "select min(salary) as min_salary from " + tableName);
        assertThat(((Number) result.value(0, "MIN_SALARY")).intValue()).isEqualTo(100);
    }

    @Test
    @DisplayName("TC_125_008 PERCENTILE_DISC returns discrete percentile values")
    void tc125008PercentileDisc() {
        String tableName = createAnalyticSampleTable("QA_FUNC_PDISC");

        QueryResult result = jdbc.query(connection, "select percentile_disc(0.5) within group (order by salary asc) as percentile_disc_value from " + tableName);
        assertThat(((Number) result.value(0, "PERCENTILE_DISC_VALUE")).intValue()).isEqualTo(200);
    }

    @Test
    @DisplayName("TC_125_009 PERCENTILE_CONT returns continuous percentile values")
    void tc125009PercentileCont() {
        String tableName = createAnalyticSampleTable("QA_FUNC_PCONT");

        QueryResult result = jdbc.query(connection, "select percentile_cont(0.5) within group (order by salary asc) as percentile_cont_value from " + tableName);
        assertThat(((Number) result.value(0, "PERCENTILE_CONT_VALUE")).doubleValue()).isEqualTo(225d);
    }

    @Test
    @DisplayName("TC_125_010 RATIO_TO_REPORT returns partition ratios")
    void tc125010RatioToReport() {
        String tableName = createAnalyticSampleTable("QA_FUNC_RATIO");

        QueryResult result = jdbc.query(
                connection,
                "select e_lastname, dno, salary, ratio_to_report(salary) over (partition by dno) as ratio_value " +
                        "from " + tableName + " order by dno, salary"
        );

        assertThat(((Number) result.value(0, "RATIO_VALUE")).doubleValue()).isCloseTo(0.16666666666666666d, within(0.0000000001d));
        assertThat(((Number) result.value(2, "RATIO_VALUE")).doubleValue()).isCloseTo(0.5d, within(0.0000000001d));
        assertThat(((Number) result.value(5, "RATIO_VALUE")).doubleValue()).isCloseTo(0.4666666666666667d, within(0.0000000001d));
    }

    @Test
    @DisplayName("TC_125_011 STDDEV function returns standard deviation")
    void tc125011Stddev() {
        String tableName = createAnalyticSampleTable("QA_FUNC_STDDEV");

        QueryResult result = jdbc.query(connection, "select stddev(salary) as stddev_value from " + tableName);
        assertThat(((Number) result.value(0, "STDDEV_VALUE")).doubleValue()).isCloseTo(93.54143d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_125_012 SUM window testcase returns total")
    void tc125012SumWindowCase() {
        String tableName = createAnalyticSampleTable("QA_FUNC_SUM_WIN");

        QueryResult result = jdbc.query(connection, "select sum(salary) as sum_salary from " + tableName);
        assertThat(((Number) result.value(0, "SUM_SALARY")).intValue()).isEqualTo(1350);
    }

    @Test
    @DisplayName("TC_125_013 VARIANCE window testcase returns variance")
    void tc125013VarianceWindowCase() {
        String tableName = createAnalyticSampleTable("QA_FUNC_VARIANCE_WIN");

        QueryResult result = jdbc.query(connection, "select variance(salary) as variance_value from " + tableName);
        assertThat(((Number) result.value(0, "VARIANCE_VALUE")).doubleValue()).isEqualTo(8750d);
    }

    @Test
    @DisplayName("TC_125_014 GROUP_CONCAT returns delimiter-joined strings")
    void tc125014GroupConcat() {
        String tableName = createAnalyticSampleTable("QA_FUNC_GCONCAT");

        QueryResult result = jdbc.query(
                connection,
                "select cast(group_concat(e_lastname, '|') as varchar(100)) as names " +
                        "from " + tableName + " group by dno order by dno"
        );

        assertThat(String.valueOf(result.value(0, "NAMES"))).contains("KIM").contains("LEE").contains("PARK").contains("|");
        assertThat(String.valueOf(result.value(1, "NAMES"))).contains("CHOI").contains("HAN").contains("SONG").contains("|");
    }

    @Test
    @DisplayName("TC_110_001 GROUP_CONCAT function returns delimiter-joined strings")
    void tc110001GroupConcat() {
        String tableName = createAnalyticSampleTable("QA_FUNC_GCONCAT_DIRECT");

        QueryResult result = jdbc.query(
                connection,
                "select cast(group_concat(e_lastname, '|') as varchar(100)) as names " +
                        "from " + tableName + " group by dno order by dno"
        );

        assertThat(String.valueOf(result.value(0, "NAMES"))).contains("KIM").contains("LEE").contains("PARK").contains("|");
        assertThat(String.valueOf(result.value(1, "NAMES"))).contains("CHOI").contains("HAN").contains("SONG").contains("|");
    }

    @Test
    @DisplayName("TC_128_001 ABS function returns absolute value")
    void tc128001Abs() {
        String value = jdbc.queryForString(connection, "select abs(-11) as abs_value from dual");
        assertThat(value).isEqualTo("11");
    }

    @Test
    @DisplayName("TC_129_001 ACOS function returns arc cosine and handles out-of-range input")
    void tc129001Acos() {
        QueryResult result = jdbc.query(
                connection,
                "select round(acos(.3), 5) as in_range_value, acos(1.00001) as out_of_range_value from dual"
        );
        assertThat(((Number) result.value(0, "IN_RANGE_VALUE")).doubleValue()).isCloseTo(1.2661d, within(0.0001d));
        assertThat(((Number) result.value(0, "OUT_OF_RANGE_VALUE")).doubleValue()).isEqualTo(0d);
    }

    @Test
    @DisplayName("TC_130_001 ASIN function returns arc sine and handles out-of-range input")
    void tc130001Asin() {
        QueryResult result = jdbc.query(
                connection,
                "select round(asin(.3), 5) as in_range_value, asin(1.00001) as out_of_range_value from dual"
        );
        assertThat(((Number) result.value(0, "IN_RANGE_VALUE")).doubleValue()).isCloseTo(0.30469d, within(0.00001d));
        assertThat(((Number) result.value(0, "OUT_OF_RANGE_VALUE")).doubleValue()).isEqualTo(0d);
    }

    @Test
    @DisplayName("TC_132_001 ATAN2 function returns arc tangent based on two inputs")
    void tc132001Atan2() {
        QueryResult result = jdbc.query(connection, "select round(atan2(.3, .2), 5) as atan2_value from dual");
        assertThat(((Number) result.value(0, "ATAN2_VALUE")).doubleValue()).isCloseTo(0.98279d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_131_001 ATAN function returns arc tangent")
    void tc131001Atan() {
        QueryResult result = jdbc.query(connection, "select round(atan(0), 5) as atan_value from dual");
        assertThat(((Number) result.value(0, "ATAN_VALUE")).doubleValue()).isEqualTo(0d);
    }

    @Test
    @DisplayName("TC_133_001 CEIL function rounds up")
    void tc133001Ceil() {
        String value = jdbc.queryForString(connection, "select ceil(12.3) as ceil_value from dual");
        assertThat(value).isEqualTo("13");
    }

    @Test
    @DisplayName("TC_134_001 COS function returns cosine")
    void tc134001Cos() {
        QueryResult result = jdbc.query(connection, "select round(cos(0), 5) as cos_value from dual");
        assertThat(((Number) result.value(0, "COS_VALUE")).doubleValue()).isEqualTo(1d);
    }

    @Test
    @DisplayName("TC_135_001 COSH function returns hyperbolic cosine")
    void tc135001Cosh() {
        QueryResult result = jdbc.query(connection, "select round(cosh(.5), 5) as cosh_value from dual");
        assertThat(((Number) result.value(0, "COSH_VALUE")).doubleValue()).isCloseTo(1.12763d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_136_001 EXP function returns exponentiation of e")
    void tc136001Exp() {
        QueryResult result = jdbc.query(connection, "select round(exp(0), 5) as exp_value from dual");
        assertThat(((Number) result.value(0, "EXP_VALUE")).doubleValue()).isEqualTo(1d);
    }

    @Test
    @DisplayName("TC_137_001 FLOOR function rounds down")
    void tc137001Floor() {
        String value = jdbc.queryForString(connection, "select floor(12.9) as floor_value from dual");
        assertThat(value).isEqualTo("12");
    }

    @Test
    @DisplayName("TC_138_001 ISNUMERIC function validates numeric text")
    void tc138001IsNumeric() {
        QueryResult result = jdbc.query(connection, "select isnumeric('1.4') as n1, isnumeric('1.4*48') as n2 from dual");
        assertThat(((Number) result.value(0, "N1")).intValue()).isEqualTo(1);
        assertThat(((Number) result.value(0, "N2")).intValue()).isEqualTo(0);
    }

    @Test
    @DisplayName("TC_139_001 LN function returns natural logarithm")
    void tc139001Ln() {
        QueryResult result = jdbc.query(connection, "select round(ln(exp(1)), 5) as ln_value from dual");
        assertThat(String.valueOf(result.value(0, "LN_VALUE"))).isEqualTo("1");
    }

    @Test
    @DisplayName("TC_140_001 LOG function returns logarithm with base")
    void tc140001Log() {
        QueryResult result = jdbc.query(connection, "select round(log(10, 100), 5) as log_value from dual");
        assertThat(String.valueOf(result.value(0, "LOG_VALUE"))).isEqualTo("2");
    }

    @Test
    @DisplayName("TC_141_001 MOD function returns remainder")
    void tc141001Mod() {
        QueryResult result = jdbc.query(connection, "select mod(10, 3) as mod_value from dual");
        assertThat(((Number) result.value(0, "MOD_VALUE")).intValue()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_142_001 NUMAND function returns bitwise AND results")
    void tc142001NumAnd() {
        QueryResult result = jdbc.query(connection, "select numand(10, 3) as n1, numand(3, 27) as n2 from dual");
        assertThat(((Number) result.value(0, "N1")).longValue()).isEqualTo(2L);
        assertThat(((Number) result.value(0, "N2")).longValue()).isEqualTo(3L);
    }

    @Test
    @DisplayName("TC_143_001 NUMOR function returns bitwise OR results")
    void tc143001NumOr() {
        QueryResult result = jdbc.query(connection, "select numor(10, 3) as n1, numor(3, 27) as n2 from dual");
        assertThat(((Number) result.value(0, "N1")).longValue()).isEqualTo(11L);
        assertThat(((Number) result.value(0, "N2")).longValue()).isEqualTo(27L);
    }

    @Test
    @DisplayName("TC_144_001 NUMSHIFT function shifts numeric bits")
    void tc144001NumShift() {
        QueryResult result = jdbc.query(connection, "select numshift(10, 3) as right_shift_value, numshift(3, -5) as left_shift_value from dual");
        assertThat(((Number) result.value(0, "RIGHT_SHIFT_VALUE")).longValue()).isEqualTo(1L);
        assertThat(((Number) result.value(0, "LEFT_SHIFT_VALUE")).longValue()).isEqualTo(96L);
    }

    @Test
    @DisplayName("TC_145_001 NUMXOR function returns bitwise XOR results")
    void tc145001NumXor() {
        QueryResult result = jdbc.query(connection, "select numxor(10, 3) as n1, numxor(3, 27) as n2 from dual");
        assertThat(((Number) result.value(0, "N1")).longValue()).isEqualTo(9L);
        assertThat(((Number) result.value(0, "N2")).longValue()).isEqualTo(24L);
    }

    @Test
    @DisplayName("TC_146_001 POWER function returns exponent result")
    void tc146001Power() {
        QueryResult result = jdbc.query(connection, "select power(2, 3) as power_value from dual");
        assertThat(((Number) result.value(0, "POWER_VALUE")).doubleValue()).isEqualTo(8d);
    }

    @Test
    @DisplayName("TC_147_001 RAND function returns values between zero and one")
    void tc147001Rand() {
        QueryResult result = jdbc.query(connection, "select rand() as rand_value from dual");
        double value = ((Number) result.value(0, "RAND_VALUE")).doubleValue();
        assertThat(value).isGreaterThanOrEqualTo(0d).isLessThan(1d);
    }

    @Test
    @DisplayName("TC_148_001 RANDOM function returns deterministic values for the same seed")
    void tc148001Random() {
        QueryResult result = jdbc.query(connection, "select random(1) as r1, random(1) as r2 from dual");
        long r1 = ((Number) result.value(0, "R1")).longValue();
        long r2 = ((Number) result.value(0, "R2")).longValue();

        assertThat(r1).isEqualTo(r2);
        assertThat(r1).isBetween(0L, 2_147_483_647L);
    }

    @Test
    @DisplayName("TC_149_001 ROUND function rounds at the requested scale")
    void tc149001Round() {
        QueryResult result = jdbc.query(connection, "select round(123.9995, 3) as r1, round(123.9995, -1) as r2 from dual");
        assertThat(((Number) result.value(0, "R1")).doubleValue()).isEqualTo(124d);
        assertThat(((Number) result.value(0, "R2")).doubleValue()).isEqualTo(120d);
    }

    @Test
    @DisplayName("TC_150_001 SIGN function returns sign indicator")
    void tc150001Sign() {
        QueryResult result = jdbc.query(connection, "select sign(15) as p1, sign(0) as p2, sign(-15) as p3 from dual");
        assertThat(((Number) result.value(0, "P1")).intValue()).isEqualTo(1);
        assertThat(((Number) result.value(0, "P2")).intValue()).isEqualTo(0);
        assertThat(((Number) result.value(0, "P3")).intValue()).isEqualTo(-1);
    }

    @Test
    @DisplayName("TC_151_001 SIN function returns sine values")
    void tc151001Sin() {
        QueryResult result = jdbc.query(connection, "select round(sin(30 * 3.14159265359 / 180), 5) as sin_value from dual");
        assertThat(((Number) result.value(0, "SIN_VALUE")).doubleValue()).isCloseTo(0.5d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_152_001 SINH function returns hyperbolic sine")
    void tc152001Sinh() {
        QueryResult result = jdbc.query(connection, "select round(sinh(1), 5) as sinh_value from dual");
        assertThat(((Number) result.value(0, "SINH_VALUE")).doubleValue()).isCloseTo(1.1752d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_153_001 SQRT function returns square root")
    void tc153001Sqrt() {
        QueryResult result = jdbc.query(connection, "select round(sqrt(144), 5) as sqrt_value from dual");
        assertThat(((Number) result.value(0, "SQRT_VALUE")).doubleValue()).isEqualTo(12d);
    }

    @Test
    @DisplayName("TC_154_001 TAN function returns tangent values")
    void tc154001Tan() {
        QueryResult result = jdbc.query(connection, "select round(tan(135 * 3.14159265359 / 180), 5) as tan_value from dual");
        assertThat(((Number) result.value(0, "TAN_VALUE")).doubleValue()).isCloseTo(-1d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_155_001 TANH function returns hyperbolic tangent")
    void tc155001Tanh() {
        QueryResult result = jdbc.query(connection, "select round(tanh(.5), 5) as tanh_value from dual");
        assertThat(((Number) result.value(0, "TANH_VALUE")).doubleValue()).isCloseTo(0.46212d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_157_001 BITAND function returns bitwise AND results")
    void tc157001BitAnd() {
        QueryResult result = jdbc.query(
                connection,
                "select to_char(bitand(bit'01010101', bit'10101010')) as b1, to_char(bitand(bit'01100101', bit'10101010')) as b2 from dual"
        );
        assertThat(String.valueOf(result.value(0, "B1"))).isEqualTo("00000000");
        assertThat(String.valueOf(result.value(0, "B2"))).isEqualTo("00100000");
    }

    @Test
    @DisplayName("TC_158_001 BITOR function returns bitwise OR results")
    void tc158001BitOr() {
        String value = jdbc.queryForString(connection, "select to_char(bitor(bit'01010101', bit'10101010')) as bit_or_value from dual");
        assertThat(value).isEqualTo("11111111");
    }

    @Test
    @DisplayName("TC_159_001 BITXOR function returns bitwise XOR results")
    void tc159001BitXor() {
        String value = jdbc.queryForString(connection, "select to_char(bitxor(bit'01010101', bit'10101010')) as bit_xor_value from dual");
        assertThat(value).isEqualTo("11111111");
    }

    @Test
    @DisplayName("TC_160_001 BITNOT function returns bitwise NOT results")
    void tc160001BitNot() {
        String value = jdbc.queryForString(connection, "select to_char(bitnot(bit'01010101')) as bit_not_value from dual");
        assertThat(value).isEqualTo("10101010");
    }

    @Test
    @DisplayName("TC_156_001 TRUNC function truncates at the requested scale")
    void tc156001Trunc() {
        QueryResult result = jdbc.query(connection, "select trunc(15.79, 1) as t1, trunc(15.79, -1) as t2 from dual");
        assertThat(((Number) result.value(0, "T1")).doubleValue()).isEqualTo(15.7d);
        assertThat(((Number) result.value(0, "T2")).doubleValue()).isEqualTo(10d);
    }

    @Test
    @DisplayName("TC_161_001 CHR function converts ASCII codes to characters")
    void tc161001Chr() {
        String value = jdbc.queryForString(connection, "select chr(65) || chr(76) || chr(84) || chr(73) || chr(66) || chr(65) || chr(83) || chr(69) as word from dual");
        assertThat(value).isEqualTo("ALTIBASE");
    }

    @Test
    @DisplayName("TC_161_002 ASCII function returns the code for the first character")
    void tc161002Ascii() {
        QueryResult result = jdbc.query(connection, "select ascii('A') as ascii_value from dual");
        assertThat(((Number) result.value(0, "ASCII_VALUE")).intValue()).isEqualTo(65);
    }

    @Test
    @DisplayName("TC_161_004 CONCAT function concatenates strings")
    void tc161004Concat() {
        String value = jdbc.queryForString(connection, "select concat('99999999_', concat('test', '%#')) as concat_value from dual");
        assertThat(value).isEqualTo("99999999_test%#");
    }

    @Test
    @DisplayName("TC_161_006 INITCAP function capitalizes each word")
    void tc161006Initcap() {
        String value = jdbc.queryForString(connection, "select initcap('the soap') as cap_value from dual");
        assertThat(value).isEqualTo("The Soap");
    }

    @Test
    @DisplayName("TC_161_007 LOWER function lowercases text")
    void tc161007Lower() {
        String value = jdbc.queryForString(connection, "select lower('ONE PAGE PROPOSAL') as lower_value from dual");
        assertThat(value).isEqualTo("one page proposal");
    }

    @Test
    @DisplayName("TC_161_008 LPAD function pads text on the left")
    void tc161008Lpad() {
        String value = jdbc.queryForString(connection, "select lpad('abc', 10, 'xyz') as lpad_value from dual");
        assertThat(value).isEqualTo("xyzxyzxabc");
    }

    @Test
    @DisplayName("TC_161_009 LTRIM function removes characters from the left")
    void tc161009Ltrim() {
        String value = jdbc.queryForString(connection, "select ltrim('abaAabLEFT', 'ab') as ltrim_value from dual");
        assertThat(value).isEqualTo("AabLEFT");
    }

    @Test
    @DisplayName("TC_161_010 PKCS7PAD16 pads strings to a 16-byte boundary")
    void tc161010Pkcs7Pad16() {
        String value = jdbc.queryForString(
                connection,
                "select length(pkcs7pad16('Altibase Client Query utility.')) as padded_length from dual"
        );
        assertThat(value).isEqualTo("32");
    }

    @Test
    @DisplayName("TC_161_011 PKCS7UNPAD16 removes PKCS7 padding")
    void tc161011Pkcs7Unpad16() {
        String value = jdbc.queryForString(
                connection,
                "select pkcs7unpad16(pkcs7pad16('Altibase Client Query utility.')) as unpadded_value from dual"
        );
        assertThat(value).isEqualTo("Altibase Client Query utility.");
    }

    @Test
    @DisplayName("TC_161_005 DIGITS function returns type-dependent fixed-width strings")
    void tc161005Digits() {
        String tableName = DbTestSupport.uniqueName("QA_FUNC_DIGITS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(i1 smallint, i2 integer, i3 bigint)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (357, 12, 5000)");

        QueryResult result = jdbc.query(connection, "select digits(i1) as d1, digits(i2) as d2, digits(i3) as d3 from " + tableName);
        assertThat(String.valueOf(result.value(0, "D1"))).hasSize(5).endsWith("357");
        assertThat(String.valueOf(result.value(0, "D2"))).hasSize(10).endsWith("12");
        assertThat(String.valueOf(result.value(0, "D3"))).hasSize(19).endsWith("5000");
    }

    @Test
    @DisplayName("TC_161_012 RANDOM_STRING function returns strings according to the requested option")
    void tc161012RandomString() {
        QueryResult result = jdbc.query(
                connection,
                "select random_string('U', 10) as u1, random_string('l', 10) as l1, random_string('p', 10) as p1 from dual"
        );

        assertThat(String.valueOf(result.value(0, "U1"))).matches("[A-Z]{10}");
        assertThat(String.valueOf(result.value(0, "L1"))).matches("[a-z]{10}");
        assertThat(String.valueOf(result.value(0, "P1"))).hasSize(10);
    }

    @Test
    @DisplayName("TC_161_013 REGEXP_COUNT function counts matching patterns")
    void tc161013RegexpCount() {
        QueryResult result = jdbc.query(
                connection,
                "select regexp_count('Daerungpost-Tower II Guro-3 Dong, Guro-gu Seoul', 'Guro', 1) as regexp_count from dual"
        );
        assertThat(((Number) result.value(0, "REGEXP_COUNT")).intValue()).isEqualTo(2);
    }

    @Test
    @DisplayName("TC_161_014 REGEXP_REPLACE function replaces the requested occurrence")
    void tc161014RegexpReplace() {
        String value = jdbc.queryForString(
                connection,
                "select regexp_replace('Daerungpost-Tower II Guro-3 Dong, Guro-gu Seoul', 'Guro', 'Mapo', 1, 2) as regexp_replace from dual"
        );
        assertThat(value).isEqualTo("Daerungpost-Tower II Guro-3 Dong, Mapo-gu Seoul");
    }

    @Test
    @DisplayName("TC_161_015 REPLICATE function repeats strings")
    void tc161015Replicate() {
        String value = jdbc.queryForString(connection, "select replicate('KSKIM', 3) as rep_value from dual");
        assertThat(value).isEqualTo("KSKIMKSKIMKSKIM");
    }

    @Test
    @DisplayName("TC_161_017 REVERSE_STR function reverses text")
    void tc161017ReverseStr() {
        String value = jdbc.queryForString(connection, "select reverse_str('KSKIM') as rev_value from dual");
        assertThat(value).isEqualTo("MIKSK");
    }

    @Test
    @DisplayName("TC_161_018 RPAD function pads text on the right")
    void tc161018Rpad() {
        String value = jdbc.queryForString(connection, "select rpad('123', 10, '0') as rpad_value from dual");
        assertThat(value).isEqualTo("1230000000");
    }

    @Test
    @DisplayName("TC_161_019 RTRIM function removes characters from the right")
    void tc161019Rtrim() {
        String value = jdbc.queryForString(connection, "select rtrim('RIGHTTRIMbaAbab', 'ab') as rtrim_value from dual");
        assertThat(value).isEqualTo("RIGHTTRIMbaA");
    }

    @Test
    @DisplayName("TC_161_020 STUFF function replaces a slice of a string")
    void tc161020Stuff() {
        String value = jdbc.queryForString(connection, "select stuff('kdhong', 2, 1, 'ildong') as stuff_value from dual");
        assertThat(value).isEqualTo("kildonghong");
    }

    @Test
    @DisplayName("TC_161_016 REPLACE2 function replaces every matching substring")
    void tc161016Replace2() {
        String value = jdbc.queryForString(connection, "select replace2('sales team support team', 'team', 'division') as replace2_value from dual");
        assertThat(value).isEqualTo("sales division support division");
    }

    @Test
    @DisplayName("TC_161_021 SUBSTR function returns the requested slice")
    void tc161021Substr() {
        String value = jdbc.queryForString(connection, "select substr('SALESMAN', 1, 5) as sub_value from dual");
        assertThat(value).isEqualTo("SALES");
    }

    @Test
    @DisplayName("TC_161_023 TRIM function removes matching characters from both sides")
    void tc161023Trim() {
        String value = jdbc.queryForString(connection, "select trim('abbAaBbAbba', 'ab') as trim_ex from dual");
        assertThat(value).isEqualTo("AaBbA");
    }

    @Test
    @DisplayName("TC_161_022 TRANSLATE function replaces matching characters")
    void tc161022Translate() {
        String value = jdbc.queryForString(connection, "select translate('MAMA', 'M', 'L') as translate_value from dual");
        assertThat(value).isEqualTo("LALA");
    }

    @Test
    @DisplayName("TC_161_024 UPPER function uppercases text")
    void tc161024Upper() {
        String value = jdbc.queryForString(connection, "select upper('Capital') as upper_value from dual");
        assertThat(value).isEqualTo("CAPITAL");
    }

    @Test
    @DisplayName("TC_162_001 ASCII function returns ASCII code values")
    void tc162001Ascii() {
        QueryResult result = jdbc.query(connection, "select ascii('G') as ascii_value from dual");
        assertThat(((Number) result.value(0, "ASCII_VALUE")).intValue()).isEqualTo(71);
    }

    @Test
    @DisplayName("TC_162_002 OCTET_LENGTH function returns byte length")
    void tc162002OctetLength() {
        QueryResult result = jdbc.query(connection, "select octet_length('test.txt') as len_value from dual");
        assertThat(((Number) result.value(0, "LEN_VALUE")).intValue()).isEqualTo(8);
    }

    @Test
    @DisplayName("TC_162_003 DIGITS function returns type-dependent fixed-width strings")
    void tc162003Digits() {
        String tableName = DbTestSupport.uniqueName("QA_FUNC_DIGITS2");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(i1 smallint, i2 integer, i3 bigint)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (357, 12, 5000)");

        QueryResult result = jdbc.query(connection, "select digits(i1) as d1, digits(i2) as d2, digits(i3) as d3 from " + tableName);
        assertThat(String.valueOf(result.value(0, "D1"))).hasSize(5).endsWith("357");
        assertThat(String.valueOf(result.value(0, "D2"))).hasSize(10).endsWith("12");
        assertThat(String.valueOf(result.value(0, "D3"))).hasSize(19).endsWith("5000");
    }

    @Test
    @DisplayName("TC_162_004 INSTR function returns match position")
    void tc162004Instr() {
        QueryResult result = jdbc.query(connection, "select instr('CORPORATE FLOOR', 'OR', 3, 2) as instr_value from dual");
        assertThat(((Number) result.value(0, "INSTR_VALUE")).intValue()).isEqualTo(14);
    }

    @Test
    @DisplayName("TC_162_005 REGEXP_INSTR function returns the starting position of a regex match")
    void tc162005RegexpInstr() {
        QueryResult result = jdbc.query(
                connection,
                "select regexp_instr('Daerungpost-Tower II Guro-3 Dong, Guro-gu Seoul', '[^ ]+', 1, 5) as regexp_instr from dual"
        );
        assertThat(((Number) result.value(0, "REGEXP_INSTR")).intValue()).isEqualTo(35);
    }

    @Test
    @DisplayName("TC_162_006 REGEXP_SUBSTR function returns the matching substring")
    void tc162006RegexpSubstr() {
        String value = jdbc.queryForString(
                connection,
                "select regexp_substr('Daerungpost-Tower II Guro-3 Dong, Guro-gu Seoul', 'Guro', 1, 2) as regexp_substr from dual"
        );
        assertThat(value).isEqualTo("Guro");
    }

    @Test
    @DisplayName("TC_162_007 SIZEOF function returns the allocated size for a column")
    void tc162007Sizeof() {
        QueryResult result = jdbc.query(connection, "select sizeof(dummy) as size_value from dual");
        assertThat(Integer.parseInt(String.valueOf(result.value(0, "SIZE_VALUE")))).isGreaterThan(0);
    }

    @Test
    @DisplayName("TC_163_001 ADD_MONTHS function shifts a date by the requested number of months")
    void tc163001AddMonths() {
        String value = jdbc.queryForString(
                connection,
                "select to_char(add_months(to_date('2024-01-15', 'YYYY-MM-DD'), 6), 'YYYY-MM-DD') as add_months_value from dual"
        );
        assertThat(value).isEqualTo("2024-07-15");
    }

    @Test
    @DisplayName("TC_164_001 DATEADD function adds units to a date")
    void tc164001DateAdd() {
        String value = jdbc.queryForString(
                connection,
                "select to_char(dateadd(to_date('2024-01-15', 'YYYY-MM-DD'), 20, 'DAY'), 'YYYY-MM-DD') as dateadd_value from dual"
        );
        assertThat(value).isEqualTo("2024-02-04");
    }

    @Test
    @DisplayName("TC_165_001 DATEDIFF function returns the difference in the requested unit")
    void tc165001DateDiff() {
        QueryResult result = jdbc.query(connection, "select datediff('31-AUG-2005', '30-NOV-2005', 'MONTH') as month_diff from dual");
        assertThat(((Number) result.value(0, "MONTH_DIFF")).intValue()).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_166_001 DATENAME function returns the requested date part name")
    void tc166001DateName() {
        String value = jdbc.queryForString(
                connection,
                "select datename('28-DEC-1980', 'MONTH') as datename_value from dual"
        );
        assertThat(value.trim()).startsWithIgnoringCase("DEC");
    }

    @Test
    @DisplayName("TC_167_001 DATEPART function returns the requested date part")
    void tc167001DatePart() {
        QueryResult result = jdbc.query(
                connection,
                "select datepart(to_date('2024-05-12', 'YYYY-MM-DD'), 'QUARTER') as quarter_value, " +
                        "datepart(to_date('2024-05-12', 'YYYY-MM-DD'), 'DAYOFYEAR') as day_of_year_value from dual"
        );
        assertThat(((Number) result.value(0, "QUARTER_VALUE")).intValue()).isEqualTo(2);
        assertThat(((Number) result.value(0, "DAY_OF_YEAR_VALUE")).intValue()).isEqualTo(133);
    }

    @Test
    @DisplayName("TC_168_001 ROUND(datetime) rounds dates by the requested unit")
    void tc168001RoundDate() {
        QueryResult result = jdbc.query(
                connection,
                "select " +
                        "to_char(round(to_date('27-DEC-1980', 'DD-MON-YYYY'), 'YEAR'), 'YYYY-MM-DD') as year_value, " +
                        "to_char(round(to_date('27-DEC-1980', 'DD-MON-YYYY'), 'MONTH'), 'YYYY-MM-DD') as month_value, " +
                        "to_char(round(to_date('27-DEC-1980', 'DD-MON-YYYY'), 'DAY'), 'YYYY-MM-DD') as day_value " +
                        "from dual"
        );
        assertThat(String.valueOf(result.value(0, "YEAR_VALUE"))).isEqualTo("1981-01-01");
        assertThat(String.valueOf(result.value(0, "MONTH_VALUE"))).isEqualTo("1981-01-01");
        assertThat(String.valueOf(result.value(0, "DAY_VALUE"))).isEqualTo("1980-12-27");
    }

    @Test
    @DisplayName("TC_169_001 LAST_DAY function returns the last day of the month")
    void tc169001LastDay() {
        String value = jdbc.queryForString(connection, "select to_char(last_day(to_date('15-DEC-2001')), 'YYYY-MM-DD') as last_day_value from dual");
        assertThat(value).isEqualTo("2001-12-31");
    }

    @Test
    @DisplayName("TC_170_001 MONTHS_BETWEEN function returns the difference in months")
    void tc170001MonthsBetween() {
        QueryResult result = jdbc.query(
                connection,
                "select round(months_between(to_date('02-02-1995','MM-DD-YYYY'), to_date('01-01-1995','MM-DD-YYYY')), 5) as months_value from dual"
        );
        assertThat(((Number) result.value(0, "MONTHS_VALUE")).doubleValue()).isCloseTo(1.03226d, within(0.00001d));
    }

    @Test
    @DisplayName("TC_171_001 NEXT_DAY function returns the first named day after the input date")
    void tc171001NextDay() {
        String value = jdbc.queryForString(
                connection,
                "select to_char(next_day(to_date('2024-05-12', 'YYYY-MM-DD'), 'SUNDAY'), 'YYYY-MM-DD') as next_day_value from dual"
        );
        assertThat(value).isEqualTo("2024-05-19");
    }

    @Test
    @DisplayName("TC_172_001 SESSION_TIMEZONE function returns the session timezone")
    void tc172001SessionTimezone() {
        String value = jdbc.queryForString(connection, "select session_timezone() as session_tz from dual");
        assertThat(value).isNotBlank();
    }

    @Test
    @DisplayName("TC_173_001 SYSDATE function returns the current system date")
    void tc173001Sysdate() {
        QueryResult result = jdbc.query(connection, "select sysdate as system_date from dual");
        assertThat(result.value(0, "SYSTEM_DATE")).isNotNull();
    }

    @Test
    @DisplayName("TC_174_001 SYSTIMESTAMP function returns the current system timestamp")
    void tc174001Systimestamp() {
        QueryResult result = jdbc.query(connection, "select systimestamp as system_ts from dual");
        assertThat(result.value(0, "SYSTEM_TS")).isNotNull();
    }

    @Test
    @DisplayName("TC_175_001 SYSDATETIME function returns the current system datetime")
    void tc175001Sysdatetime() {
        QueryResult result = jdbc.query(connection, "select sysdatetime as system_dt from dual");
        assertThat(result.value(0, "SYSTEM_DT")).isNotNull();
    }

    @Test
    @DisplayName("TC_176_001 TRUNC on SYSDATE removes time-of-day fields")
    void tc176001TruncSysdate() {
        QueryResult result = jdbc.query(connection, "select to_char(trunc(sysdate), 'YYYY-MM-DD') as d1, to_char(sysdate, 'YYYY-MM-DD') as d2 from dual");
        assertThat(String.valueOf(result.value(0, "D1"))).isEqualTo(String.valueOf(result.value(0, "D2")));
    }

    @Test
    @DisplayName("TC_177_001 UNIX_DATE function returns the current UTC-based datetime")
    void tc177001UnixDate() {
        QueryResult result = jdbc.query(
                connection,
                "select to_char(conv_timezone(unix_date, '+00:00', session_timezone()), 'YYYYMMDDHH24MI') as converted_unix_date, " +
                        "to_char(current_date, 'YYYYMMDDHH24MI') as current_date_value from dual"
        );
        assertThat(String.valueOf(result.value(0, "CONVERTED_UNIX_DATE"))).isEqualTo(String.valueOf(result.value(0, "CURRENT_DATE_VALUE")));
    }

    @Test
    @DisplayName("TC_178_001 CURRENT_DATE function returns the current session date")
    void tc178001CurrentDate() {
        String value = jdbc.queryForString(connection, "select to_char(current_date, 'YYYY MM/DD HH24:MI') as current_date_value from dual");
        assertThat(value).matches("\\d{4} \\d{2}/\\d{2} \\d{2}:\\d{2}");
    }

    @Test
    @DisplayName("TC_179_001 DB_TIMEZONE function returns the database timezone")
    void tc179001DbTimezone() {
        String value = jdbc.queryForString(connection, "select db_timezone() as db_tz from dual");
        assertThat(value).isNotBlank();
    }

    @Test
    @DisplayName("TC_180_001 CONV_TIMEZONE function converts values between time zones")
    void tc180001ConvTimezone() {
        String value = jdbc.queryForString(
                connection,
                "select to_char(conv_timezone(to_date('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), '+00:00', '+09:00'), 'YYYY-MM-DD HH24:MI') as converted_value from dual"
        );
        assertThat(value).isEqualTo("2024-01-01 09:00");
    }

    @Test
    @DisplayName("TC_225_001 LEAST function returns the smallest string")
    void tc225001Least() {
        String value = jdbc.queryForString(connection, "select least('HARRY','HARRIOT','HAROLD') as least_value from dual");
        assertThat(value).isEqualTo("HAROLD");
    }

    @Test
    @DisplayName("TC_231_001 NULLIF returns null when values are equal")
    void tc231001NullIf() {
        QueryResult result = jdbc.query(connection, "select nullif(10,10) as n1, nullif(10,9) as n2 from dual");
        assertThat(result.value(0, "N1")).isNull();
        assertThat(String.valueOf(result.value(0, "N2"))).isEqualTo("10");
    }

    @Test
    @DisplayName("TC_232_001 NVL returns default for null")
    void tc232001Nvl() {
        QueryResult result = jdbc.query(connection, "select nvl(null,10) as n1, nvl(10,9) as n2 from dual");
        assertThat(String.valueOf(result.value(0, "N1"))).isEqualTo("10");
        assertThat(String.valueOf(result.value(0, "N2"))).isEqualTo("10");
    }

    @Test
    @DisplayName("TC_233_001 NVL2 returns different values depending on nullability")
    void tc233001Nvl2() {
        QueryResult result = jdbc.query(connection, "select nvl2(null,88,213) as n1, nvl2('A',10,999) as n2 from dual");
        assertThat(String.valueOf(result.value(0, "N1"))).isEqualTo("213");
        assertThat(String.valueOf(result.value(0, "N2"))).isEqualTo("10");
    }

    @Test
    @DisplayName("TC_234_001 QUOTED_PRINTABLE_ENCODE converts RAW values to quoted-printable form")
    void tc234001QuotedPrintableEncode() {
        QueryResult result = jdbc.query(
                connection,
                "select quoted_printable_encode(varbyte'ABCD') as quoted_printable_value from dual"
        );

        assertThat(toHex((byte[]) result.value(0, "QUOTED_PRINTABLE_VALUE"))).isEqualTo("3D41423D4344");
    }

    @Test
    @DisplayName("TC_235_001 QUOTED_PRINTABLE_DECODE restores quoted-printable RAW values")
    void tc235001QuotedPrintableDecode() {
        QueryResult result = jdbc.query(
                connection,
                "select quoted_printable_decode(varbyte'3D4142') as quoted_printable_value from dual"
        );

        assertThat(toHex((byte[]) result.value(0, "QUOTED_PRINTABLE_VALUE"))).isEqualTo("AB");
    }

    @Test
    @DisplayName("TC_240_001 USER_ID function can be queried")
    void tc240001UserId() {
        assertThat(jdbc.query(connection, "select user_id() as uid from dual").size()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_241_001 USER_NAME function can be queried")
    void tc241001UserName() {
        String userName = jdbc.queryForString(connection, "select user_name() as uname from dual");
        assertThat(userName).isEqualToIgnoringCase("SYS");
    }

    @Test
    @DisplayName("TC_242_001 SESSION_ID function can be queried")
    void tc242001SessionId() {
        assertThat(jdbc.query(connection, "select session_id() as sid from dual").size()).isEqualTo(1);
    }

    private String createAnalyticSampleTable(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(sex char(1), eno int, dno int, salary int, e_lastname varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('M', 1, 10, 100, 'KIM')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('M', 2, 10, 200, 'LEE')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('M', 3, 10, 300, 'PARK')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('F', 4, 20, 150, 'CHOI')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('F', 5, 20, 250, 'HAN')");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('F', 6, 20, 350, 'SONG')");
        return tableName;
    }

    private String createOrderedSetSampleTable(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(eno int, dno int, salary int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (1, 1001, 980)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (2, 1003, 1000)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (3, 1003, 1800)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (4, 1005, 2003)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (5, 1007, 2300)");
        return tableName;
    }

    private String createKeepDenseRankSampleTable(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(eno int, dno int, salary int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (1, 10, 1000)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (2, 10, 800)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (3, 20, 900)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (4, 20, 700)");
        return tableName;
    }

    private String createAnovaSampleTable(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(group_id int, amount int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (1, 1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (1, 2)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (2, 4)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (2, 5)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (3, 7)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values (3, 8)");
        return tableName;
    }

    private String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (byte value : bytes) {
            builder.append(String.format("%02X", value));
        }
        return builder.toString();
    }
}
