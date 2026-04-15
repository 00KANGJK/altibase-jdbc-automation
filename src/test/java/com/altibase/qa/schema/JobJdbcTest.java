package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_305_001 create a monthly job")
    void tc305001CreateJob() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        assertThat(jobExists(jobName)).isTrue();
        assertThat(jobValue(jobName, "EXEC_QUERY")).isEqualTo(procedureName);
        assertThat(jobValue(jobName, "INTERVAL")).isEqualTo("1");
        assertThat(jobValue(jobName, "INTERVAL_TYPE")).isEqualTo("MM");
    }

    @Test
    @DisplayName("TC_305_002 create a daily job for a bounded period and enable it")
    void tc305002CreateDailyJobAndEnable() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(
                connection,
                "create job " + jobName + " exec " + procedureName +
                        " start to_date('2016-08-01 00:00:00','YYYY-MM-DD HH24:MI:SS')" +
                        " end to_date('2016-08-31 00:00:00','YYYY-MM-DD HH24:MI:SS')" +
                        " interval 1 day"
        );

        assertThat(jobEnabled(jobName)).isFalse();
        jdbc.executeUpdate(connection, "alter job " + jobName + " set enable");

        assertThat(jobValue(jobName, "START_TIME")).startsWith("2016-08-01 00:00:00");
        assertThat(jobValue(jobName, "END_TIME")).startsWith("2016-08-31 00:00:00");
        assertThat(jobValue(jobName, "INTERVAL")).isEqualTo("1");
        assertThat(jobValue(jobName, "INTERVAL_TYPE")).isEqualTo("DD");
        assertThat(jobEnabled(jobName)).isTrue();
    }

    @Test
    @DisplayName("TC_306_001 alter a job to enabled state")
    void tc306001AlterJob() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        assertThat(jobEnabled(jobName)).isFalse();
        jdbc.executeUpdate(connection, "alter job " + jobName + " set enable");
        assertThat(jobEnabled(jobName)).isTrue();
        jdbc.executeUpdate(connection, "alter job " + jobName + " set disable");
        assertThat(jobEnabled(jobName)).isFalse();
    }

    @Test
    @DisplayName("TC_306_002 alter a job to disabled state")
    void tc306002DisableJob() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");
        jdbc.executeUpdate(connection, "alter job " + jobName + " set enable");
        assertThat(jobEnabled(jobName)).isTrue();

        jdbc.executeUpdate(connection, "alter job " + jobName + " set disable");

        assertThat(jobEnabled(jobName)).isFalse();
    }

    @Test
    @DisplayName("TC_306_003 alter a job start date")
    void tc306003AlterJobStartDate() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        jdbc.executeUpdate(
                connection,
                "alter job " + jobName + " set start to_date('2013-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')"
        );

        assertThat(jobValue(jobName, "START_TIME")).startsWith("2013-01-01 00:00:00");
    }

    @Test
    @DisplayName("TC_306_004 alter a job to execute a different procedure")
    void tc306004AlterJobProcedure() {
        String originalProcedure = DbTestSupport.uniqueName("QA_JOB_PROC");
        String changedProcedure = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, changedProcedure));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, originalProcedure));

        createNoOpProcedure(originalProcedure);
        createNoOpProcedure(changedProcedure);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + originalProcedure + " start sysdate interval 1 month");

        jdbc.executeUpdate(connection, "alter job " + jobName + " set exec " + changedProcedure);

        assertThat(jobValue(jobName, "EXEC_QUERY")).isEqualTo(changedProcedure);
    }

    @Test
    @DisplayName("TC_306_005 alter a job start time")
    void tc306005AlterJobStartTime() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        jdbc.executeUpdate(
                connection,
                "alter job " + jobName + " set start to_date('2013-06-03 10:00:00','YYYY-MM-DD HH24:MI:SS')"
        );

        assertThat(jobValue(jobName, "START_TIME")).startsWith("2013-06-03 10:00:00");
    }

    @Test
    @DisplayName("TC_306_006 alter a job end time")
    void tc306006AlterJobEndTime() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        jdbc.executeUpdate(
                connection,
                "alter job " + jobName + " set end to_date('2013-06-07 10:00:00','YYYY-MM-DD HH24:MI:SS')"
        );

        assertThat(jobValue(jobName, "END_TIME")).startsWith("2013-06-07 10:00:00");
    }

    @Test
    @DisplayName("TC_306_007 alter a job interval to ten minutes")
    void tc306007AlterJobInterval() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        jdbc.executeUpdate(connection, "alter job " + jobName + " set interval 10 minute");

        assertThat(jobValue(jobName, "INTERVAL")).isEqualTo("10");
        assertThat(jobValue(jobName, "INTERVAL_TYPE")).isEqualTo("MI");
    }

    @Test
    @DisplayName("TC_307_001 drop a job")
    void tc307001DropJob() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");
        jdbc.executeUpdate(connection, "drop job " + jobName);

        assertThat(jobExists(jobName)).isFalse();
    }

    @Test
    @DisplayName("Additional negative case: duplicate job names are rejected")
    void duplicateJobNameFails() {
        String procedureName = DbTestSupport.uniqueName("QA_JOB_PROC");
        String jobName = DbTestSupport.uniqueName("QA_JOB");

        registerCleanup(() -> dropJobQuietly(jobName));
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        createNoOpProcedure(procedureName);
        jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "create job " + jobName + " exec " + procedureName + " start sysdate interval 1 month"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: altering a missing job fails")
    void alterMissingJobFails() {
        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "alter job MISSING_JOB set enable"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: dropping a missing job fails")
    void dropMissingJobFails() {
        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "drop job MISSING_JOB"))
                .isInstanceOf(IllegalStateException.class);
    }

    private void createNoOpProcedure(String procedureName) {
        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");
    }

    private boolean jobExists(String jobName) {
        return jdbc.exists(connection, "select job_name from system_.sys_jobs_ where job_name = '" + jobName + "'");
    }

    private boolean jobEnabled(String jobName) {
        return "T".equalsIgnoreCase(jobValue(jobName, "IS_ENABLE"));
    }

    private String jobValue(String jobName, String columnName) {
        return jdbc.queryForString(
                connection,
                "select " + columnName + " from system_.sys_jobs_ where job_name = '" + jobName + "'"
        );
    }

    private void dropJobQuietly(String jobName) {
        try {
            jdbc.executeUpdate(connection, "drop job " + jobName);
        } catch (Exception ignored) {
        }
    }
}
