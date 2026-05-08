package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import com.altibase.qa.support.DbTestSupport;
import com.altibase.qa.support.SqlExceptionSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class DataTypeBoundaryJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional boundary case: CHAR and VARCHAR round-trip exact values and reject overflow")
    void characterTypesRoundTripAndRejectOverflow() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_CHAR_RT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c_char char(5), c_varchar varchar(5))");
        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "insert into " + tableName + "(c_char, c_varchar) values(?, ?)")) {
            preparedStatement.setString(1, "A");
            preparedStatement.setString(2, "ABCDE");
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        }

        QueryResult result = jdbc.query(
                connection,
                "select rtrim(c_char) as c_char_trimmed, c_varchar from " + tableName
        );

        assertThat(result.rows()).hasSize(1);
        assertThat(result.value(0, "C_CHAR_TRIMMED")).isEqualTo("A");
        assertThat(result.value(0, "C_VARCHAR")).isEqualTo("ABCDE");

        assertSqlFails("insert into " + tableName + "(c_char, c_varchar) values('ABCDEF', 'OK')");
        assertSqlFails("insert into " + tableName + "(c_char, c_varchar) values('OK', 'ABCDEF')");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional boundary case: NCHAR and NVARCHAR preserve multibyte text and reject overflow")
    void nationalCharacterTypesRoundTripAndRejectOverflow() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_NCHAR_RT");
        String fixedValue = "\uD55C\uAE00A";
        String variableValue = "\uAC00\uB098ABC";
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c_nchar nchar(5), c_nvarchar nvarchar(5))");
        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "insert into " + tableName + "(c_nchar, c_nvarchar) values(?, ?)")) {
            preparedStatement.setString(1, fixedValue);
            preparedStatement.setString(2, variableValue);
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        }

        QueryResult result = jdbc.query(
                connection,
                "select rtrim(c_nchar) as c_nchar_trimmed, c_nvarchar from " + tableName
        );

        assertThat(result.rows()).hasSize(1);
        assertThat(result.value(0, "C_NCHAR_TRIMMED")).isEqualTo(fixedValue);
        assertThat(result.value(0, "C_NVARCHAR")).isEqualTo(variableValue);

        assertSqlFails("insert into " + tableName + "(c_nchar, c_nvarchar) values('ABCDEF', 'OK')");
        assertSqlFails("insert into " + tableName + "(c_nchar, c_nvarchar) values('OK', 'ABCDEF')");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional boundary case: exact numeric values round-trip and precision overflow is rejected")
    void numericTypesRoundTripAndRejectPrecisionOverflow() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_NUM_RT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(
                connection,
                "create table " + tableName + "(" +
                        "id integer, c_numeric numeric(6,2), c_number number, " +
                        "c_float float, c_double double, c_real real)"
        );
        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "insert into " + tableName + " values(?, ?, ?, ?, ?, ?)")) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setBigDecimal(2, new BigDecimal("9999.99"));
            preparedStatement.setBigDecimal(3, new BigDecimal("-42.25"));
            preparedStatement.setDouble(4, 12.5d);
            preparedStatement.setDouble(5, 12345.5d);
            preparedStatement.setFloat(6, 3.25f);
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        }

        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "select * from " + tableName + " where id = 1");
             ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getBigDecimal("C_NUMERIC")).isEqualByComparingTo(new BigDecimal("9999.99"));
            assertThat(resultSet.getBigDecimal("C_NUMBER")).isEqualByComparingTo(new BigDecimal("-42.25"));
            assertThat(resultSet.getDouble("C_FLOAT")).isCloseTo(12.5d, within(0.0001d));
            assertThat(resultSet.getDouble("C_DOUBLE")).isCloseTo(12345.5d, within(0.0001d));
            assertThat(resultSet.getFloat("C_REAL")).isCloseTo(3.25f, within(0.0001f));
        }

        assertSqlFails("insert into " + tableName + "(id, c_numeric) values(2, 10000.00)");
        assertSqlFails("insert into " + tableName + "(id, c_numeric) values(3, -10000.00)");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @ParameterizedTest(name = "Additional boundary case: {0} accepts stable lower bound and maximum through PreparedStatement binding")
    @CsvSource({
            "smallint,-32767,32767",
            "integer,-2147483647,2147483647",
            "bigint,-9223372036854775807,9223372036854775807"
    })
    void signedIntegerTypesAcceptStablePreparedStatementBounds(String typeName, String minValue, String maxValue) throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_BOUND_" + typeName);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 " + typeName + ")");
        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            bindSignedIntegralValue(preparedStatement, typeName, minValue);
            preparedStatement.executeUpdate();

            bindSignedIntegralValue(preparedStatement, typeName, maxValue);
            preparedStatement.executeUpdate();
        }

        QueryResult result = jdbc.query(
                connection,
                "select min(c1) as min_c1, max(c1) as max_c1 from " + tableName
        );

        assertThat(result.rows()).hasSize(1);
        assertThat(result.rows().get(0).get("MIN_C1").toString()).isEqualTo(minValue);
        assertThat(result.rows().get(0).get("MAX_C1").toString()).isEqualTo(maxValue);
    }

    @ParameterizedTest(name = "Additional negative case: {0} rejects literal values above its signed range")
    @CsvSource({
            "smallint,32768",
            "integer,2147483648",
            "bigint,9223372036854775808"
    })
    void signedIntegerTypesRejectUpperOverflowLiteral(String typeName, String overflowValue) {
        String tableName = DbTestSupport.uniqueName("QA_BOUND_UPPER_" + typeName);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 " + typeName + ")");

        assertSqlFails("insert into " + tableName + " values(" + overflowValue + ")");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
    }

    @ParameterizedTest(name = "Additional defect case: {0} literal exact minimum overflows")
    @CsvSource({
            "smallint,-32768",
            "integer,-2147483648",
            "bigint,-9223372036854775808"
    })
    void signedIntegerTypesRejectExactMinimumLiteral(String typeName, String minValue) {
        String tableName = DbTestSupport.uniqueName("QA_BOUND_LIT_" + typeName);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 " + typeName + ")");

        assertThatThrownBy(() -> jdbc.executeUpdate(connection, "insert into " + tableName + " values(" + minValue + ")"))
                .isInstanceOf(IllegalStateException.class)
                .satisfies(throwable -> assertThat(SqlExceptionSupport.requireSqlException(throwable).getMessage())
                        .contains("Value overflow"));

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("0");
    }

    @ParameterizedTest(name = "Additional defect case: {0} bound exact minimum is stored as NULL")
    @CsvSource({
            "smallint,-32768",
            "integer,-2147483648",
            "bigint,-9223372036854775808"
    })
    void signedIntegerTypesBindExactMinimumAsNull(String typeName, String minValue) throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_BOUND_BIND_" + typeName);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 " + typeName + ")");

        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            bindSignedIntegralValue(preparedStatement, typeName, minValue);
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
        assertThat(jdbc.queryForString(connection, "select count(c1) from " + tableName)).isEqualTo("0");
        assertThat(jdbc.queryForString(connection, "select c1 from " + tableName)).isNull();
    }

    @Test
    @DisplayName("Additional boundary case: DATE preserves leap-day timestamp values and rejects invalid dates")
    void dateTypeRoundTripsLeapDayAndRejectsInvalidDate() {
        String tableName = DbTestSupport.uniqueName("QA_DATE_RT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 date)");
        jdbc.executeUpdate(
                connection,
                "insert into " + tableName + " values(to_date('2024-02-29 23:59:58','YYYY-MM-DD HH24:MI:SS'))"
        );

        assertThat(jdbc.queryForString(
                connection,
                "select to_char(c1, 'YYYY-MM-DD HH24:MI:SS') from " + tableName
        )).isEqualTo("2024-02-29 23:59:58");

        assertSqlFails("insert into " + tableName + " values(to_date('2023-02-29','YYYY-MM-DD'))");
        assertSqlFails("insert into " + tableName + " values(to_date('2024-13-01','YYYY-MM-DD'))");
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional boundary case: BYTE and VARBYTE preserve bytes and reject declared length overflow")
    void byteTypesRoundTripAndRejectOverflow() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_BYTE_RT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c_byte byte(3), c_varbyte varbyte(3))");
        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "insert into " + tableName + "(c_byte, c_varbyte) values(?, ?)")) {
            preparedStatement.setBytes(1, new byte[]{0x01, 0x23, 0x45});
            preparedStatement.setBytes(2, new byte[]{0x0A, 0x0B, 0x0C});
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
        }

        QueryResult result = jdbc.query(
                connection,
                "select to_char(c_byte) as c_byte_hex, to_char(c_varbyte) as c_varbyte_hex from " + tableName
        );

        assertThat(result.rows()).hasSize(1);
        assertThat(result.value(0, "C_BYTE_HEX")).isEqualTo("012345");
        assertThat(result.value(0, "C_VARBYTE_HEX")).isEqualTo("0A0B0C");

        assertJdbcFails(() -> {
            try (PreparedStatement preparedStatement =
                         jdbc.prepare(connection, "insert into " + tableName + "(c_byte, c_varbyte) values(?, ?)")) {
                preparedStatement.setBytes(1, new byte[]{0x01, 0x02, 0x03, 0x04});
                preparedStatement.setBytes(2, new byte[]{0x01});
                preparedStatement.executeUpdate();
            }
        });
        assertJdbcFails(() -> {
            try (PreparedStatement preparedStatement =
                         jdbc.prepare(connection, "insert into " + tableName + "(c_byte, c_varbyte) values(?, ?)")) {
                preparedStatement.setBytes(1, new byte[]{0x01});
                preparedStatement.setBytes(2, new byte[]{0x01, 0x02, 0x03, 0x04});
                preparedStatement.executeUpdate();
            }
        });
        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional boundary case: BLOB and CLOB round-trip stream contents")
    void lobTypesRoundTripStreamContents() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_LOB_RT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c_blob blob, c_clob clob)");

        jdbc.begin(connection);
        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "insert into " + tableName + "(c_blob, c_clob) values(?, ?)")) {
            preparedStatement.setBinaryStream(1, new ByteArrayInputStream(new byte[]{0x01, 0x02, 0x03}), 3);
            preparedStatement.setCharacterStream(2, new StringReader("clob-\uD55C\uAE00"), 7);
            assertThat(preparedStatement.executeUpdate()).isEqualTo(1);
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "select c_blob, c_clob from " + tableName);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next()).isTrue();
            try (var blobStream = resultSet.getBinaryStream("C_BLOB");
                 Reader clobReader = resultSet.getCharacterStream("C_CLOB")) {
                assertThat(blobStream.readAllBytes()).containsExactly((byte) 0x01, (byte) 0x02, (byte) 0x03);
                assertThat(readAll(clobReader)).isEqualTo("clob-\uD55C\uAE00");
            }
        }
    }

    @Test
    @DisplayName("Additional manual case: DECIMAL columns are exposed through JDBC metadata")
    void decimalColumnsAreVisibleThroughMetadata() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_DEC_META");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 decimal(10,2))");

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(null, null, tableName, "C1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TYPE_NAME")).isIn("DECIMAL", "NUMERIC");
            assertThat(rs.getInt("COLUMN_SIZE")).isEqualTo(10);
            assertThat(rs.getInt("DECIMAL_DIGITS")).isEqualTo(2);
        }
    }

    @Test
    @DisplayName("Additional manual case: NVARCHAR preserves multibyte text through PreparedStatement round-trip")
    void nvarcharRoundTripsMultibyteText() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_NVARCHAR_RT");
        String sample = "\uD55C\uAE00-\u03A9-\u00DF";
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 nvarchar(20))");
        try (PreparedStatement preparedStatement =
                     jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            preparedStatement.setString(1, sample);
            preparedStatement.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select c1 from " + tableName)).isEqualTo(sample);
    }

    @Test
    @DisplayName("Additional manual case: ORDER BY supports NULLS FIRST and NULLS LAST")
    void orderBySupportsNullsFirstAndLast() {
        String tableName = DbTestSupport.uniqueName("QA_NULL_ORDER");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(2)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(NULL)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

        List<String> nullsFirstOrder = jdbc.query(
                        connection,
                        "select c1 from " + tableName + " order by c1 nulls first"
                ).rows().stream()
                .map(row -> row.get("C1") == null ? "NULL" : row.get("C1").toString())
                .collect(Collectors.toList());

        List<String> nullsLastOrder = jdbc.query(
                        connection,
                        "select c1 from " + tableName + " order by c1 nulls last"
                ).rows().stream()
                .map(row -> row.get("C1") == null ? "NULL" : row.get("C1").toString())
                .collect(Collectors.toList());

        assertThat(nullsFirstOrder).containsExactly("NULL", "1", "2");
        assertThat(nullsLastOrder).containsExactly("1", "2", "NULL");
    }

    private void bindSignedIntegralValue(PreparedStatement preparedStatement, String typeName, String value) throws Exception {
        switch (typeName.toLowerCase(Locale.ROOT)) {
            case "smallint" -> preparedStatement.setShort(1, Short.parseShort(value));
            case "integer" -> preparedStatement.setInt(1, Integer.parseInt(value));
            case "bigint" -> preparedStatement.setLong(1, Long.parseLong(value));
            default -> throw new IllegalArgumentException("Unsupported signed integer type: " + typeName);
        }
    }

    private void assertSqlFails(String sql) {
        assertThatThrownBy(() -> jdbc.executeUpdate(connection, sql))
                .isInstanceOf(IllegalStateException.class)
                .satisfies(throwable -> assertThat((Object) SqlExceptionSupport.findSqlException(throwable)).isNotNull());
    }

    private void assertJdbcFails(org.assertj.core.api.ThrowableAssert.ThrowingCallable callable) {
        assertThatThrownBy(callable)
                .satisfies(throwable -> assertThat((Object) SqlExceptionSupport.findSqlException(throwable)).isNotNull());
    }

    private String readAll(Reader reader) throws Exception {
        StringBuilder builder = new StringBuilder();
        char[] buffer = new char[256];
        int read;
        while ((read = reader.read(buffer)) != -1) {
            builder.append(buffer, 0, read);
        }
        return builder.toString();
    }
}
