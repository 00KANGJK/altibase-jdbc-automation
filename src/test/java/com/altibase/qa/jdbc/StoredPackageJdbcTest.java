package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import com.altibase.qa.support.FeatureProbe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StoredPackageJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional manual case: DBMS_RANDOM returns numeric random values")
    void dbmsRandomReturnsNumericValues() {
        FeatureProbe.assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "DBMS_RANDOM",
                "select dbms_random.random() as random_value from dual"
        );

        QueryResult result = jdbc.query(
                connection,
                "select dbms_random.random() as random_integer, dbms_random.value(10, 20) as bounded_value from dual"
        );

        assertThat(result.value(0, "RANDOM_INTEGER")).isInstanceOf(Number.class);
        double boundedValue = ((Number) result.value(0, "BOUNDED_VALUE")).doubleValue();
        assertThat(boundedValue).isGreaterThanOrEqualTo(10d).isLessThan(20d);
    }

    @Test
    @DisplayName("Additional manual case: UTL_RAW converts between INTEGER, RAW, and VARCHAR")
    void utlRawConvertsBetweenIntegerRawAndVarchar() {
        FeatureProbe.assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "UTL_RAW",
                "select utl_raw.cast_from_binary_integer(123456) as raw_value from dual"
        );

        QueryResult result = jdbc.query(
                connection,
                "select utl_raw.cast_from_binary_integer(123456) as raw_value, " +
                        "utl_raw.cast_to_binary_integer('40E20100') as integer_value, " +
                        "utl_raw.cast_to_varchar2(utl_raw.cast_to_raw('altibase')) as varchar_value from dual"
        );

        assertThat(toHex((byte[]) result.value(0, "RAW_VALUE"))).isEqualTo("40E20100");
        assertThat(((Number) result.value(0, "INTEGER_VALUE")).intValue()).isEqualTo(123456);
        assertThat(result.value(0, "VARCHAR_VALUE")).isEqualTo("altibase");
    }

    @Test
    @DisplayName("Additional manual case: UTL_RAW CONCAT, SUBSTR, and LENGTH manipulate RAW data")
    void utlRawConcatSubstrAndLengthManipulateRawData() {
        FeatureProbe.assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "UTL_RAW",
                "select utl_raw.length(utl_raw.cast_to_raw('A')) as raw_length from dual"
        );

        QueryResult result = jdbc.query(
                connection,
                "select utl_raw.concat(raw'AA', raw'BB') as concat_value, " +
                        "utl_raw.substr('0102030405', 1, 2) as substr_value, " +
                        "utl_raw.length(utl_raw.cast_to_raw('altibase')) as raw_length from dual"
        );

        assertThat(toHex((byte[]) result.value(0, "CONCAT_VALUE"))).isEqualTo("AABB");
        assertThat(toHex((byte[]) result.value(0, "SUBSTR_VALUE"))).isEqualTo("0102");
        assertThat(((Number) result.value(0, "RAW_LENGTH")).intValue()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Additional negative case: UTL_RAW rejects invalid RAW text")
    void utlRawRejectsInvalidRawText() {
        FeatureProbe.assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "UTL_RAW",
                "select utl_raw.cast_to_binary_integer('40E20100') as integer_value from dual"
        );

        assertThatThrownBy(() ->
                jdbc.query(connection, "select utl_raw.cast_to_binary_integer('XYZ') from dual"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional manual case: DBMS_OUTPUT can be invoked from a stored procedure")
    void dbmsOutputCanBeInvokedFromStoredProcedure() {
        FeatureProbe.assumeStoredPackageAvailable(
                config,
                jdbc,
                connection,
                "DBMS_OUTPUT",
                "select proc_name from system_.sys_procedures_ where proc_name = 'DBMS_OUTPUT'"
        );

        String procedureName = com.altibase.qa.support.DbTestSupport.uniqueName("QA_DBMS_OUTPUT");
        registerCleanup(() -> com.altibase.qa.support.DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 out integer) as begin " +
                        "dbms_output.put_line('altibase'); " +
                        "p1 := 1; " +
                        "end;"
        );

        assertThat(jdbc.queryForString(connection,
                "select proc_name from system_.sys_procedures_ where proc_name = '" + procedureName + "'"))
                .isEqualTo(procedureName);
    }

    private String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (byte value : bytes) {
            builder.append(String.format("%02X", value));
        }
        return builder.toString();
    }
}
