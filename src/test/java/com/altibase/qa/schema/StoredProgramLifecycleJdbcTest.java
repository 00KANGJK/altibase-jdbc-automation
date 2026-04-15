package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class StoredProgramLifecycleJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_270_001 create a procedure without parameters")
    void tc270001CreateProcedureWithoutParameters() {
        String procedureName = newProcedure("QA_PROC_NOPARAM", "as begin null; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_002 create a procedure with IN parameters")
    void tc270002CreateProcedureWithInParameters() {
        String procedureName = newProcedure("QA_PROC_IN", "(p1 in integer, p2 in varchar(20)) as begin null; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_003 create a procedure with OUT parameters")
    void tc270003CreateProcedureWithOutParameters() {
        String procedureName = newProcedure("QA_PROC_OUT", "(p1 out integer) as begin p1 := 1; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_004 create a procedure with IN OUT parameters")
    void tc270004CreateProcedureWithInOutParameters() {
        String procedureName = newProcedure("QA_PROC_INOUT", "(p1 in out integer) as begin p1 := p1 + 1; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_005 create a procedure with local variables")
    void tc270005CreateProcedureWithLocalVariables() {
        String procedureName = newProcedure("QA_PROC_LOCAL", "(p1 out integer) as v1 integer; begin v1 := 10; p1 := v1; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_006 create a procedure containing DML")
    void tc270006CreateProcedureWithDml() {
        String tableName = DbTestSupport.uniqueName("QA_PROC_DML_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));
        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        String procedureName = newProcedure("QA_PROC_DML", "(p1 in integer) as begin insert into " + tableName + " values(p1); end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_007 created procedure is stored in the catalog")
    void tc270007StoredProcedureExistsInCatalog() {
        String procedureName = newProcedure("QA_PROC_STORE", "as begin null; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_008 parameterized procedure is stored in the catalog")
    void tc270008StoredParameterizedProcedureExistsInCatalog() {
        String procedureName = newProcedure("QA_PROC_STORE_IN", "(p1 in integer) as begin null; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_009 OUT procedure is stored in the catalog")
    void tc270009StoredOutProcedureExistsInCatalog() {
        String procedureName = newProcedure("QA_PROC_STORE_OUT", "(p1 out integer) as begin p1 := 1; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_270_010 replaced procedure remains stored in the catalog")
    void tc270010ReplacedProcedureRemainsStored() {
        String procedureName = DbTestSupport.uniqueName("QA_PROC_REPLACE");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");
        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " as begin null; end;");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_271_001 alter procedure compile")
    void tc271001AlterProcedureCompile() {
        String procedureName = newProcedure("QA_PROC_COMPILE", "(p1 out integer) as begin p1 := 1; end;");
        jdbc.executeUpdate(connection, "alter procedure " + procedureName + " compile");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_271_002 alter a parameterized procedure by compiling it")
    void tc271002AlterParameterizedProcedureCompile() {
        String procedureName = newProcedure("QA_PROC_COMPILE2", "(p1 in integer, p2 out integer) as begin p2 := p1 + 1; end;");
        jdbc.executeUpdate(connection, "alter procedure " + procedureName + " compile");
        assertThat(procedureExists(procedureName)).isTrue();
    }

    @Test
    @DisplayName("TC_272_001 drop procedure")
    void tc272001DropProcedure() {
        String procedureName = newProcedure("QA_PROC_DROP", "(p1 out integer) as begin p1 := 1; end;");
        jdbc.executeUpdate(connection, "drop procedure " + procedureName);
        assertThat(procedureExists(procedureName)).isFalse();
    }

    @Test
    @DisplayName("TC_273_001 execute procedure")
    void tc273001ExecuteProcedure() throws Exception {
        String procedureName = newProcedure("QA_PROC_EXEC", "(p1 in integer, p2 out integer) as begin p2 := p1 + 100; end;");
        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?,?)}")) {
            cs.setInt(1, 7);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            assertThat(cs.getInt(2)).isEqualTo(107);
        }
    }

    @Test
    @DisplayName("TC_274_001 create a simple function")
    void tc274001CreateSimpleFunction() {
        String functionName = newFunction("QA_FUNC_SIMPLE", "(f1 in integer) return number as begin return f1; end;");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_274_002 create a function with arithmetic")
    void tc274002CreateArithmeticFunction() {
        String functionName = newFunction("QA_FUNC_ARITH", "(f1 in integer, f2 in integer) return number as begin return f1 + f2; end;");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_274_003 create a function with local variables")
    void tc274003CreateFunctionWithLocalVariables() {
        String functionName = newFunction("QA_FUNC_LOCAL", "(f1 in integer) return number as v1 number; begin v1 := f1 + 10; return v1; end;");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_274_004 create a function returning character data")
    void tc274004CreateCharacterFunction() {
        String functionName = newFunction("QA_FUNC_CHAR", "(f1 in varchar(20)) return varchar(20) as begin return f1; end;");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_274_005 create a function using SELECT INTO")
    void tc274005CreateSelectIntoFunction() {
        String tableName = DbTestSupport.uniqueName("QA_FUNC_SEL_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));
        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(11)");
        String functionName = newFunction("QA_FUNC_SEL", " return number as v1 number; begin select c1 into v1 from " + tableName + "; return v1; end;");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_274_006 create a function containing DML")
    void tc274006CreateFunctionWithDml() {
        String tableName = DbTestSupport.uniqueName("QA_FUNC_DML_TB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));
        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        String functionName = newFunction("QA_FUNC_DML", "(f1 in integer) return number as begin insert into " + tableName + " values(f1); return f1; end;");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_274_007 create a function with multiple parameters")
    void tc274007CreateFunctionWithMultipleParameters() {
        String functionName = newFunction("QA_FUNC_MULTI", "(f1 in integer, f2 in integer, f3 in integer) return number as begin return f1 + f2 + f3; end;");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_274_008 create a function without parameters")
    void tc274008CreateFunctionWithoutParameters() {
        String functionName = newFunction("QA_FUNC_NOPARAM", " return number as begin return 1; end;");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_275_001 alter function compile")
    void tc275001AlterFunctionCompile() {
        String functionName = newFunction("QA_FUNC_COMPILE", "(f1 in integer) return number as begin return f1 + 1; end;");
        jdbc.executeUpdate(connection, "alter function " + functionName + " compile");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_275_002 alter a character function by compiling it")
    void tc275002AlterCharacterFunctionCompile() {
        String functionName = newFunction("QA_FUNC_COMPILE2", "(f1 in varchar(20)) return varchar(20) as begin return f1; end;");
        jdbc.executeUpdate(connection, "alter function " + functionName + " compile");
        assertThat(functionExists(functionName)).isTrue();
    }

    @Test
    @DisplayName("TC_276_001 drop function")
    void tc276001DropFunction() {
        String functionName = newFunction("QA_FUNC_DROP", "(f1 in integer) return number as begin return f1; end;");
        jdbc.executeUpdate(connection, "drop function " + functionName);
        assertThat(functionExists(functionName)).isFalse();
    }

    @Test
    @DisplayName("TC_277_001 create a delete trigger that logs deleted rows")
    void tc277001CreateDeleteTrigger() {
        String sourceTable = DbTestSupport.uniqueName("QA_ORDERS");
        String logTable = DbTestSupport.uniqueName("QA_LOG");
        String triggerName = DbTestSupport.uniqueName("QA_DEL_TRG");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, logTable));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, sourceTable));

        jdbc.executeUpdate(connection, "create table " + sourceTable + "(id integer)");
        jdbc.executeUpdate(connection, "create table " + logTable + "(id integer)");
        jdbc.executeUpdate(connection,
                "create or replace trigger " + triggerName + " after delete on " + sourceTable +
                        " referencing old row old_row for each row as begin insert into " + logTable + " values(old_row.id); end;");
        jdbc.executeUpdate(connection, "insert into " + sourceTable + " values(1)");
        jdbc.executeUpdate(connection, "delete from " + sourceTable + " where id = 1");

        assertThat(jdbc.queryForString(connection, "select id from " + logTable)).isEqualTo("1");
    }

    @Test
    @DisplayName("TC_277_002 create a BEFORE INSERT trigger")
    void tc277002CreateBeforeInsertTrigger() {
        String tableName = DbTestSupport.uniqueName("QA_TRG_TB");
        String triggerName = DbTestSupport.uniqueName("QA_INS_TRG");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        createSimpleInsertTrigger(triggerName, tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c2) values(1, null)");

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1")).isEqualTo("0");
    }

    @Test
    @DisplayName("TC_277_003 create a disabled trigger and then enable it")
    void tc277003CreateDisabledTriggerThenEnable() {
        String tableName = DbTestSupport.uniqueName("QA_TRG_DIS_TB");
        String triggerName = DbTestSupport.uniqueName("QA_TRG_DIS");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        createSimpleInsertTriggerDisabled(triggerName, tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c2) values(1, null)");
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1")).isNull();

        jdbc.executeUpdate(connection, "alter trigger " + triggerName + " enable");
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c2) values(2, null)");
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 2")).isEqualTo("0");
    }

    @Test
    @DisplayName("TC_278_001 alter a trigger to disable it")
    void tc278001AlterTriggerDisable() {
        String tableName = DbTestSupport.uniqueName("QA_TRG_ALT_TB");
        String triggerName = DbTestSupport.uniqueName("QA_TRG_ALT");
        registerCleanup(() -> DbTestSupport.dropTriggerQuietly(jdbc, connection, triggerName));
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        createSimpleInsertTrigger(triggerName, tableName);
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c2) values(1, null)");
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1")).isEqualTo("0");

        jdbc.executeUpdate(connection, "alter trigger " + triggerName + " disable");
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c2) values(2, null)");
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 2")).isNull();
    }

    @Test
    @DisplayName("TC_279_001 drop trigger")
    void tc279001DropTrigger() {
        String tableName = DbTestSupport.uniqueName("QA_TRG_DROP_TB");
        String triggerName = DbTestSupport.uniqueName("QA_TRG_DROP");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer, c2 integer)");
        createSimpleInsertTrigger(triggerName, tableName);
        jdbc.executeUpdate(connection, "drop trigger " + triggerName);
        jdbc.executeUpdate(connection, "insert into " + tableName + "(c1, c2) values(1, null)");

        assertThat(triggerExists(triggerName)).isFalse();
        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1")).isNull();
    }

    private String newProcedure(String prefix, String definition) {
        String procedureName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        jdbc.executeUpdate(connection, "create or replace procedure " + procedureName + " " + definition);
        return procedureName;
    }

    private String newFunction(String prefix, String definition) {
        String functionName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropFunctionQuietly(jdbc, connection, functionName));
        jdbc.executeUpdate(connection, "create or replace function " + functionName + " " + definition);
        return functionName;
    }

    private boolean procedureExists(String name) {
        return jdbc.exists(connection, "select proc_name from system_.sys_procedures_ where proc_name = '" + name + "'");
    }

    private boolean functionExists(String name) {
        return jdbc.exists(connection, "select proc_name from system_.sys_procedures_ where proc_name = '" + name + "'");
    }

    private boolean triggerExists(String name) {
        return jdbc.exists(connection, "select trigger_name from system_.sys_triggers_ where trigger_name = '" + name + "'");
    }

    private void createSimpleInsertTrigger(String triggerName, String tableName) {
        jdbc.executeUpdate(connection,
                "create or replace trigger " + triggerName + " before insert on " + tableName +
                        " referencing new row new_row for each row as begin if new_row.c2 is null then new_row.c2 := 0; end if; end;");
    }

    private void createSimpleInsertTriggerDisabled(String triggerName, String tableName) {
        jdbc.executeUpdate(connection,
                "create or replace trigger " + triggerName + " before insert on " + tableName +
                        " referencing new row new_row for each row disable as begin if new_row.c2 is null then new_row.c2 := 0; end if; end;");
    }
}
