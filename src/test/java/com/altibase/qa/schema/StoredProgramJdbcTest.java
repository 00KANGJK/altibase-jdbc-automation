package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class StoredProgramJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_428_001 INSERT procedure can be created")
    void tc428001CreateInsertProcedure() {
        String tableName = DbTestSupport.uniqueName("QA_EMP_TB");
        String procedureName = DbTestSupport.uniqueName("QA_EMP_PROC");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createEmployeeTable(tableName);
        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 in integer, p2 in char(20), p3 in char(20), p4 in char(1)) " +
                        "as begin insert into " + tableName + "(eno, e_firstname, e_lastname, sex) values(p1, p2, p3, p4); end;"
        );

        assertThat(storedProgramExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_428_002 INSERT procedure can be executed")
    void tc428002ExecuteInsertProcedure() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_EMP_TB");
        String procedureName = DbTestSupport.uniqueName("QA_EMP_PROC");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createEmployeeTable(tableName);
        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 in integer, p2 in char(20), p3 in char(20), p4 in char(1)) " +
                        "as begin insert into " + tableName + "(eno, e_firstname, e_lastname, sex) values(p1, p2, p3, p4); end;"
        );

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?,?,?,?)}")) {
            cs.setInt(1, 21);
            cs.setString(2, "Joel");
            cs.setString(3, "Johnson");
            cs.setString(4, "M");
            cs.execute();
        }

        assertThat(jdbc.queryForString(connection, "select eno from " + tableName + " where eno = 21")).isEqualTo("21");
        assertThat(jdbc.queryForString(connection, "select trim(e_firstname) from " + tableName + " where eno = 21")).isEqualTo("Joel");
        assertThat(jdbc.queryForString(connection, "select trim(e_lastname) from " + tableName + " where eno = 21")).isEqualTo("Johnson");
        assertThat(jdbc.queryForString(connection, "select trim(sex) from " + tableName + " where eno = 21")).isEqualTo("M");
    }

    @Test
    @DisplayName("TC_428_003 SELECT procedure with OUT parameters can be created")
    void tc428003CreateOutProcedure() {
        String tableName = DbTestSupport.uniqueName("QA_OUT_TB");
        String procedureName = DbTestSupport.uniqueName("QA_OUT_PROC");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createOutTable(tableName);
        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(a1 out integer, a2 in out integer) as begin " +
                        "select count(*) into a1 from " + tableName + " where i2 = a2; end;"
        );

        assertThat(storedProgramExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_428_004 SELECT procedure with OUT parameters can be executed")
    void tc428004ExecuteOutProcedure() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_OUT_TB");
        String procedureName = DbTestSupport.uniqueName("QA_OUT_PROC");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createOutTable(tableName);
        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(a1 out integer, a2 in out integer) as begin " +
                        "select count(*) into a1 from " + tableName + " where i2 = a2; end;"
        );

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?,?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.setInt(2, 1);
            cs.execute();

            assertThat(cs.getInt(1)).isEqualTo(5);
            assertThat(cs.getInt(2)).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("TC_428_005 OUT/IN OUT procedure can be created")
    void tc428005CreateOutProcedureVariant() {
        String procedureName = DbTestSupport.uniqueName("QA_OUT_PROC1");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 integer, p2 in out integer, p3 out integer) " +
                        "as begin p2 := p1; p3 := p1 + 100; end;"
        );

        assertThat(storedProgramExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_428_006 IN OUT SELECT procedure can be created")
    void tc428006CreateInOutSelectProcedure() {
        String tableName = DbTestSupport.uniqueName("QA_INOUT_TB");
        String procedureName = DbTestSupport.uniqueName("QA_INOUT_PROC");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(i1 integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(a1 in out integer) as begin " +
                        "select count(*) into a1 from " + tableName + " where i1 = a1; end;"
        );

        assertThat(storedProgramExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_428_007 arithmetic IN OUT procedure can be created")
    void tc428007CreateInOutArithmeticProcedure() {
        String procedureName = DbTestSupport.uniqueName("QA_INOUT_PROC1");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 integer, p2 in out integer, p3 out integer) " +
                        "as begin p2 := p1 + p2; p3 := p1 + 100; end;"
        );

        assertThat(storedProgramExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_428_008 arithmetic IN OUT procedure can be executed")
    void tc428008ExecuteInOutArithmeticProcedure() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_INOUT_PROC1");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 integer, p2 in out integer, p3 out integer) " +
                        "as begin p2 := p1 + p2; p3 := p1 + 100; end;"
        );

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?,?,?)}")) {
            cs.setInt(1, 3);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.setInt(2, 5);
            cs.registerOutParameter(3, Types.INTEGER);
            cs.execute();

            assertThat(cs.getInt(2)).isEqualTo(8);
            assertThat(cs.getInt(3)).isEqualTo(103);
        }
    }

    @Test
    @DisplayName("TC_429_001 procedure can be dropped")
    void tc429001DropProcedure() {
        String procedureName = DbTestSupport.uniqueName("QA_DROP_PROC");
        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + "(p1 out integer) as begin p1 := 1; end;");
        jdbc.executeUpdate(connection, "drop procedure " + procedureName);

        assertThat(storedProgramExists(procedureName)).isFalse();
    }

    @Test
    @DisplayName("TC_430_001 function can be created")
    void tc430001CreateFunction() {
        String tableName = DbTestSupport.uniqueName("QA_FUNC_EMP");
        String functionName = DbTestSupport.uniqueName("QA_EMP_FUNC");
        registerCleanup(() -> DbTestSupport.dropFunctionQuietly(jdbc, connection, functionName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createEmployeeTable(tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + "(eno, salary) values(11, 5000)");
        jdbc.executeUpdate(
                connection,
                "create or replace function " + functionName + "(f1 in integer) return number as f2 number; begin " +
                        "update " + tableName + " set salary = 1000000 where eno = f1; " +
                        "select salary into f2 from " + tableName + " where eno = f1; return f2; end;"
        );

        assertThat(storedProgramExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_431_001 function can be executed")
    void tc431001ExecuteFunction() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_FUNC_EMP");
        String functionName = DbTestSupport.uniqueName("QA_EMP_FUNC");
        registerCleanup(() -> DbTestSupport.dropFunctionQuietly(jdbc, connection, functionName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        createEmployeeTable(tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + "(eno, salary) values(11, 5000)");
        jdbc.executeUpdate(
                connection,
                "create or replace function " + functionName + "(f1 in integer) return number as f2 number; begin " +
                        "update " + tableName + " set salary = 1000000 where eno = f1; " +
                        "select salary into f2 from " + tableName + " where eno = f1; return f2; end;"
        );

        try (CallableStatement cs = connection.prepareCall("{ ? = call " + functionName + "(?) }")) {
            cs.registerOutParameter(1, Types.NUMERIC);
            cs.setInt(2, 11);
            cs.execute();

            assertThat(cs.getBigDecimal(1)).isNotNull();
            assertThat(cs.getBigDecimal(1).intValue()).isEqualTo(1000000);
        }
        assertThat(jdbc.queryForString(connection, "select salary from " + tableName + " where eno = 11")).isEqualTo("1000000");
    }

    @Test
    @DisplayName("TC_432_001 function can be dropped")
    void tc432001DropFunction() {
        String functionName = DbTestSupport.uniqueName("QA_DROP_FUNC");
        jdbc.executeUpdate(connection, "create or replace function " + functionName + "(f1 in integer) return number as begin return f1; end;");
        jdbc.executeUpdate(connection, "drop function " + functionName);

        assertThat(storedProgramExists(functionName)).isFalse();
    }

    private void createEmployeeTable(String tableName) {
        jdbc.executeUpdate(
                connection,
                "create table " + tableName + "(eno integer, e_firstname char(20), e_lastname char(20), sex char(1), salary integer)"
        );
    }

    private void createOutTable(String tableName) {
        jdbc.executeUpdate(connection, "create table " + tableName + "(i1 integer, i2 integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 1)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1, 1)");
    }

    private boolean storedProgramExists(String name) {
        return jdbc.exists(
                connection,
                "select proc_name from system_.sys_procedures_ where proc_name = '" + name + "'"
        );
    }
}
