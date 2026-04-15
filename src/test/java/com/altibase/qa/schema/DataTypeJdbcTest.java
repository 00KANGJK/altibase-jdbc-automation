package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataTypeJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_249_001 CHAR data type is supported")
    void tc249001Char() throws Exception {
        assertTypeName("char(10)", "CHAR");
    }

    @Test
    @DisplayName("TC_250_001 VARCHAR data type is supported")
    void tc250001Varchar() throws Exception {
        assertTypeName("varchar(10)", "VARCHAR");
    }

    @Test
    @DisplayName("TC_251_001 NCHAR data type is supported")
    void tc251001Nchar() throws Exception {
        assertTypeName("nchar(10)", "NCHAR");
    }

    @Test
    @DisplayName("TC_252_001 NVARCHAR data type is supported")
    void tc252001Nvarchar() throws Exception {
        assertTypeName("nvarchar(10)", "NVARCHAR");
    }

    @Test
    @DisplayName("TC_253_001 NUMERIC data type is supported")
    void tc253001Numeric() throws Exception {
        assertTypeName("numeric(10,2)", "NUMERIC");
    }

    @Test
    @DisplayName("TC_254_001 NUMBER data type is supported")
    void tc254001Number() throws Exception {
        assertTypeNameIn("number", "FLOAT", "NUMBER");
    }

    @Test
    @DisplayName("TC_255_001 FLOAT data type is supported")
    void tc255001Float() throws Exception {
        assertTypeName("float", "FLOAT");
    }

    @Test
    @DisplayName("TC_256_001 DOUBLE data type is supported")
    void tc256001Double() throws Exception {
        assertTypeName("double", "DOUBLE");
    }

    @Test
    @DisplayName("TC_257_001 REAL data type is supported")
    void tc257001Real() throws Exception {
        assertTypeName("real", "REAL");
    }

    @Test
    @DisplayName("TC_258_001 BIGINT data type is supported")
    void tc258001Bigint() throws Exception {
        assertTypeName("bigint", "BIGINT");
    }

    @Test
    @DisplayName("TC_259_001 INTEGER data type is supported")
    void tc259001Integer() throws Exception {
        assertTypeName("integer", "INTEGER");
    }

    @Test
    @DisplayName("TC_260_001 SMALLINT data type is supported")
    void tc260001Smallint() throws Exception {
        assertTypeName("smallint", "SMALLINT");
    }

    @Test
    @DisplayName("TC_261_001 DATE data type is supported")
    void tc261001Date() throws Exception {
        assertTypeName("date", "DATE");
    }

    @Test
    @DisplayName("TC_262_001 BLOB data type is supported")
    void tc262001Blob() throws Exception {
        assertTypeName("blob", "BLOB");
    }

    @Test
    @DisplayName("TC_263_001 CLOB data type is supported")
    void tc263001Clob() throws Exception {
        assertTypeName("clob", "CLOB");
    }

    @Test
    @DisplayName("TC_264_001 BYTE data type is supported")
    void tc264001Byte() throws Exception {
        assertTypeName("byte(10)", "BYTE");
    }

    @Test
    @DisplayName("TC_265_001 VARBYTE data type is supported")
    void tc265001Varbyte() throws Exception {
        assertTypeName("varbyte(10)", "VARBYTE");
    }

    @Test
    @DisplayName("TC_266_001 NIBBLE data type is supported")
    void tc266001Nibble() throws Exception {
        assertTypeName("nibble(10)", "NIBBLE");
    }

    @Test
    @DisplayName("TC_267_001 BIT data type is supported")
    void tc267001Bit() throws Exception {
        assertTypeName("bit(10)", "BIT");
    }

    @Test
    @DisplayName("TC_268_001 VARBIT data type is supported")
    void tc268001Varbit() throws Exception {
        assertTypeName("varbit(10)", "VARBIT");
    }

    @Test
    @DisplayName("TC_269_001 GEOMETRY data type is supported")
    void tc269001Geometry() throws Exception {
        assertTypeName("geometry", "GEOMETRY");
    }

    @Test
    @DisplayName("Additional negative case: VARCHAR length is enforced")
    void varcharLengthOverflowFails() {
        String tableName = DbTestSupport.uniqueName("QA_VARCHAR_FAIL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(3))");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "insert into " + tableName + " values('TOOLONG')"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: invalid DATE literals are rejected")
    void invalidDateLiteralFails() {
        String tableName = DbTestSupport.uniqueName("QA_DATE_FAIL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 date)");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection,
                        "insert into " + tableName + " values(to_date('2025-13-01','YYYY-MM-DD'))"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: NUMERIC precision is enforced")
    void numericPrecisionOverflowFails() {
        String tableName = DbTestSupport.uniqueName("QA_NUM_FAIL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 numeric(4,0))");

        assertThatThrownBy(() ->
                jdbc.executeUpdate(connection, "insert into " + tableName + " values(12345)"))
                .isInstanceOf(IllegalStateException.class);
    }

    private void assertTypeName(String columnDefinition, String expectedTypeName) throws Exception {
        String typeName = createTableAndReadType(columnDefinition);
        assertThat(typeName).isEqualTo(expectedTypeName);
    }

    private void assertTypeNameIn(String columnDefinition, String... expectedTypeNames) throws Exception {
        String typeName = createTableAndReadType(columnDefinition);
        assertThat(typeName).isIn((Object[]) expectedTypeNames);
    }

    private String createTableAndReadType(String columnDefinition) throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_TYPE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 " + columnDefinition + ")");

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(null, null, tableName, "C1")) {
            assertThat(rs.next()).isTrue();
            return rs.getString("TYPE_NAME");
        }
    }
}
