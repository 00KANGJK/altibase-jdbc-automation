package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class CallableStatementJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_674_001 CallableStatement getBigDecimal(int) returns OUT values")
    void tc674001GetBigDecimal() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_BDEC");
        createProcedure(procedureName, "numeric(10,3)", "12.345");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.NUMERIC);
            cs.execute();

            assertThat(cs.getBigDecimal(1)).isEqualByComparingTo(new BigDecimal("12.345"));
        }
    }

    @Test
    @DisplayName("TC_675_001 CallableStatement getBigDecimal(int, scale) returns scaled OUT values")
    void tc675001GetBigDecimalWithScale() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_BDEC");
        createProcedure(procedureName, "numeric(10,3)", "12.345");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.NUMERIC);
            cs.execute();

            assertThat(cs.getBigDecimal(1, 2)).isEqualByComparingTo(new BigDecimal("12.34"));
        }
    }

    @Test
    @DisplayName("TC_676_001 CallableStatement getDate(int) returns OUT date values")
    void tc676001GetDate() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_DATE");
        createProcedure(procedureName, "date", "to_date('2024-01-02', 'YYYY-MM-DD')");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.DATE);
            cs.execute();

            assertThat(cs.getDate(1)).isEqualTo(Date.valueOf("2024-01-02"));
        }
    }

    @Test
    @DisplayName("TC_677_001 CallableStatement getDouble(int) returns OUT double values")
    void tc677001GetDouble() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_DBL");
        createProcedure(procedureName, "double", "12.5");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.DOUBLE);
            cs.execute();

            assertThat(cs.getDouble(1)).isEqualTo(12.5d);
        }
    }

    @Test
    @DisplayName("TC_678_001 CallableStatement getFloat(int) returns OUT float values")
    void tc678001GetFloat() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_FLT");
        createProcedure(procedureName, "float", "9.5");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.FLOAT);
            cs.execute();

            assertThat(cs.getFloat(1)).isEqualTo(9.5f);
        }
    }

    @Test
    @DisplayName("TC_679_001 CallableStatement getInt(int) returns OUT integer values")
    void tc679001GetInt() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_INT");
        createProcedure(procedureName, "integer", "77");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();

            assertThat(cs.getInt(1)).isEqualTo(77);
        }
    }

    @Test
    @DisplayName("TC_680_001 CallableStatement getLong(int) returns OUT long values")
    void tc680001GetLong() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_LONG");
        createProcedure(procedureName, "bigint", "123456789");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.BIGINT);
            cs.execute();

            assertThat(cs.getLong(1)).isEqualTo(123456789L);
        }
    }

    @Test
    @DisplayName("TC_681_001 CallableStatement getObject(int) returns OUT object values")
    void tc681001GetObject() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_OBJ");
        createProcedure(procedureName, "integer", "7");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();

            assertThat(cs.getObject(1)).isEqualTo(7);
        }
    }

    @Test
    @DisplayName("TC_682_001 CallableStatement getString(int) returns OUT string values")
    void tc682001GetString() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_STR");
        createProcedure(procedureName, "varchar(20)", "'ALTIBASE'");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.execute();

            assertThat(cs.getString(1)).isEqualTo("ALTIBASE");
        }
    }

    @Test
    @DisplayName("TC_683_001 CallableStatement getTime(int) returns OUT time values")
    void tc683001GetTime() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_TIME");
        createProcedure(procedureName, "date", "to_date('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS')");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.TIME);
            cs.execute();

            assertThat(cs.getTime(1)).isEqualTo(Time.valueOf("03:04:05"));
        }
    }

    @Test
    @DisplayName("TC_684_001 CallableStatement getTimestamp(int) returns OUT timestamp values")
    void tc684001GetTimestamp() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_TS");
        createProcedure(procedureName, "date", "to_date('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS')");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.TIMESTAMP);
            cs.execute();

            assertThat(cs.getTimestamp(1)).isEqualTo(Timestamp.valueOf("2024-01-02 03:04:05"));
        }
    }

    @Test
    @DisplayName("TC_685_001 CallableStatement registerOutParameter registers OUT parameters")
    void tc685001RegisterOutParameter() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_REG");
        createProcedure(procedureName, "integer", "42");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();

            assertThat(cs.getInt(1)).isEqualTo(42);
        }
    }

    @Test
    @DisplayName("TC_686_001 CallableStatement wasNull reports null OUT values")
    void tc686001WasNull() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CALL_NULL");
        createProcedure(procedureName, "varchar(20)", "null");

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.execute();

            assertThat(cs.getString(1)).isNull();
            assertThat(cs.wasNull()).isTrue();
        }
    }

    private void createProcedure(String procedureName, String type, String assignment) {
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 out " + type + ") as begin p1 := " + assignment + "; end;"
        );
    }
}
