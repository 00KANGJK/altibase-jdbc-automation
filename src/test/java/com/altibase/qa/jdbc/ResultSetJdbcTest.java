package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import javax.sql.rowset.serial.SerialBlob;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResultSetJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_559_001 ResultSet getAsciiStream(int) returns an ASCII input stream")
    void tc559001GetAsciiStreamByIndex() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_ASCII");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('ASCII-TEXT')");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            try (InputStream inputStream = rs.getAsciiStream(1)) {
                assertThat(new String(inputStream.readAllBytes())).isEqualTo("ASCII-TEXT");
            }
        }
    }

    @Test
    @DisplayName("TC_560_001 ResultSet getAsciiStream(String) returns an ASCII input stream")
    void tc560001GetAsciiStreamByName() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_ASCII_NM");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('ASCII-BY-NAME')");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            try (InputStream inputStream = rs.getAsciiStream("C1")) {
                assertThat(new String(inputStream.readAllBytes())).isEqualTo("ASCII-BY-NAME");
            }
        }
    }

    @Test
    @DisplayName("TC_561_001 ResultSet getBigDecimal(int) returns decimal values")
    void tc561001GetBigDecimalByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select cast(12.345 as numeric(10,3)) from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal(1)).isEqualByComparingTo(new BigDecimal("12.345"));
        }
    }

    @Test
    @DisplayName("TC_562_001 ResultSet getBigDecimal(int, scale) returns scaled decimal values")
    void tc562001GetBigDecimalByIndexWithScale() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select cast(12.345 as numeric(10,3)) from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal(1, 2)).isEqualByComparingTo(new BigDecimal("12.34"));
        }
    }

    @Test
    @DisplayName("TC_563_001 ResultSet getBigDecimal(String) returns decimal values")
    void tc563001GetBigDecimalByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select cast(12.345 as numeric(10,3)) as n1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("N1")).isEqualByComparingTo(new BigDecimal("12.345"));
        }
    }

    @Test
    @DisplayName("TC_564_001 ResultSet getBigDecimal(String, scale) returns scaled decimal values")
    void tc564001GetBigDecimalByNameWithScale() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select cast(12.345 as numeric(10,3)) as n1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("N1", 2)).isEqualByComparingTo(new BigDecimal("12.34"));
        }
    }

    @Test
    @DisplayName("TC_565_001 ResultSet getBinaryStream(int) returns a binary input stream")
    void tc565001GetBinaryStreamByIndex() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_BIN_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 blob)");
        insertBlob(tableName, new byte[]{0x01, 0x02, 0x03});

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            try (InputStream inputStream = rs.getBinaryStream(1)) {
                assertThat(inputStream.readAllBytes()).containsExactly((byte) 0x01, (byte) 0x02, (byte) 0x03);
            }
        }
    }

    @Test
    @DisplayName("TC_566_001 ResultSet getBinaryStream(String) returns a binary input stream")
    void tc566001GetBinaryStreamByName() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_BIN_NM");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 blob)");
        insertBlob(tableName, new byte[]{0x0A, 0x0B});

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            try (InputStream inputStream = rs.getBinaryStream("C1")) {
                assertThat(inputStream.readAllBytes()).containsExactly((byte) 0x0A, (byte) 0x0B);
            }
        }
    }

    @Test
    @DisplayName("TC_567_001 ResultSet getBlob(int) returns a blob instance")
    void tc567001GetBlobByIndex() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_BLOB_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 blob)");
        insertBlob(tableName, new byte[]{0x01, 0x02, 0x03, 0x04});

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            Blob blob = rs.getBlob(1);
            assertThat(blob.length()).isEqualTo(4);
        }
    }

    @Test
    @DisplayName("TC_568_001 ResultSet getBlob(String) returns a blob instance")
    void tc568001GetBlobByName() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_BLOB_NM");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 blob)");
        insertBlob(tableName, new byte[]{0x0A});

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            Blob blob = rs.getBlob("C1");
            assertThat(blob.length()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("TC_569_001 ResultSet getBoolean(int) returns boolean values")
    void tc569001GetBooleanByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 1 as flag from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBoolean(1)).isTrue();
        }
    }

    @Test
    @DisplayName("TC_570_001 ResultSet getBoolean(String) returns boolean values")
    void tc570001GetBooleanByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 1 as flag from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBoolean("FLAG")).isTrue();
        }
    }

    @Test
    @DisplayName("TC_571_001 ResultSet getCharacterStream(int) returns a reader for text columns")
    void tc571001GetCharacterStreamByIndex() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_CHAR_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('altibase')");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            try (Reader reader = rs.getCharacterStream(1)) {
                assertThat(readAll(reader)).isEqualTo("altibase");
            }
        }
    }

    @Test
    @DisplayName("TC_572_001 ResultSet getCharacterStream(String) returns a reader for text columns")
    void tc572001GetCharacterStreamByName() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_CHAR_NM");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(20))");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values ('jdbc-reader')");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            try (Reader reader = rs.getCharacterStream("C1")) {
                assertThat(readAll(reader)).isEqualTo("jdbc-reader");
            }
        }
    }

    @Test
    @DisplayName("TC_579_001 ResultSet getInt(int) returns integer values")
    void tc579001GetIntByIndex() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RS_INT_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("TC_580_001 ResultSet getInt(String) returns integer values")
    void tc580001GetIntByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 100 as n1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("N1")).isEqualTo(100);
        }
    }

    @Test
    @DisplayName("TC_575_001 ResultSet getDouble(int) returns double values")
    void tc575001GetDoubleByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 12.5 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getDouble(1)).isEqualTo(12.5d);
        }
    }

    @Test
    @DisplayName("TC_576_001 ResultSet getDouble(String) returns double values")
    void tc576001GetDoubleByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 12.5 as d1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getDouble("D1")).isEqualTo(12.5d);
        }
    }

    @Test
    @DisplayName("TC_577_001 ResultSet getFloat(int) returns float values")
    void tc577001GetFloatByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 9.5 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getFloat(1)).isEqualTo(9.5f);
        }
    }

    @Test
    @DisplayName("TC_578_001 ResultSet getFloat(String) returns float values")
    void tc578001GetFloatByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 9.5 as f1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getFloat("F1")).isEqualTo(9.5f);
        }
    }

    @Test
    @DisplayName("TC_583_001 ResultSet getMetaData returns metadata")
    void tc583001GetMetaData() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_id() as uid, user_name() as uname from dual")) {
            assertThat(rs.getMetaData()).isNotNull();
            assertThat(rs.getMetaData().getColumnCount()).isEqualTo(2);
        }
    }

    @Test
    @DisplayName("TC_584_001 ResultSet getObject(int) returns object values")
    void tc584001GetObjectByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 100 from dual")) {
            assertThat(rs.next()).isTrue();
            Object value = rs.getObject(1);
            assertThat(value).isInstanceOf(Number.class);
            assertThat(((Number) value).intValue()).isEqualTo(100);
        }
    }

    @Test
    @DisplayName("TC_585_001 ResultSet getObject(String) returns object values")
    void tc585001GetObjectByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_name() as uname from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(String.valueOf(rs.getObject("UNAME"))).isEqualToIgnoringCase("SYS");
        }
    }

    @Test
    @DisplayName("TC_586_001 ResultSet getRow returns current row number")
    void tc586001GetRow() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getRow()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("TC_581_001 ResultSet getLong(int) returns long values")
    void tc581001GetLongByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 123456789 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(123456789L);
        }
    }

    @Test
    @DisplayName("TC_582_001 ResultSet getLong(String) returns long values")
    void tc582001GetLongByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 123456789 as l1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("L1")).isEqualTo(123456789L);
        }
    }

    @Test
    @DisplayName("TC_589_001 ResultSet getStatement returns the creating statement")
    void tc589001GetStatement() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_name() from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getStatement()).isSameAs(stmt);
        }
    }

    @Test
    @DisplayName("TC_590_001 ResultSet getString(int) returns string values")
    void tc590001GetStringByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_name() from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualToIgnoringCase("SYS");
        }
    }

    @Test
    @DisplayName("TC_591_001 ResultSet getString(String) returns string values")
    void tc591001GetStringByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_name() as uname from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("UNAME")).isEqualToIgnoringCase("SYS");
        }
    }

    @Test
    @DisplayName("TC_573_001 ResultSet getDate(int) returns date values")
    void tc573001GetDateByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select to_date('2024-01-02', 'YYYY-MM-DD') as d1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getDate(1)).isEqualTo(Date.valueOf("2024-01-02"));
        }
    }

    @Test
    @DisplayName("TC_574_001 ResultSet getDate(String) returns date values")
    void tc574001GetDateByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select to_date('2024-01-02', 'YYYY-MM-DD') as d1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getDate("D1")).isEqualTo(Date.valueOf("2024-01-02"));
        }
    }

    @Test
    @DisplayName("TC_587_001 ResultSet getShort(int) returns short values")
    void tc587001GetShortByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 12 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getShort(1)).isEqualTo((short) 12);
        }
    }

    @Test
    @DisplayName("TC_588_001 ResultSet getShort(String) returns short values")
    void tc588001GetShortByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 12 as s1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getShort("S1")).isEqualTo((short) 12);
        }
    }

    @Test
    @DisplayName("TC_592_001 ResultSet getTime(int) returns time values")
    void tc592001GetTimeByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select to_date('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS') as t1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getTime(1)).isEqualTo(Time.valueOf("03:04:05"));
        }
    }

    @Test
    @DisplayName("TC_593_001 ResultSet getTime(String) returns time values")
    void tc593001GetTimeByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select to_date('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS') as t1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getTime("T1")).isEqualTo(Time.valueOf("03:04:05"));
        }
    }

    @Test
    @DisplayName("TC_594_001 ResultSet getTimestamp(int) returns timestamp values")
    void tc594001GetTimestampByIndex() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select to_date('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS') as ts1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getTimestamp(1)).isEqualTo(Timestamp.valueOf("2024-01-02 03:04:05"));
        }
    }

    @Test
    @DisplayName("TC_595_001 ResultSet getTimestamp(String) returns timestamp values")
    void tc595001GetTimestampByName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select to_date('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS') as ts1 from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getTimestamp("TS1")).isEqualTo(Timestamp.valueOf("2024-01-02 03:04:05"));
        }
    }

    @Test
    @DisplayName("TC_596_001 ResultSet getFetchDirection returns fetch forward")
    void tc596001GetFetchDirection() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            assertThat(rs.getFetchDirection()).isEqualTo(ResultSet.FETCH_FORWARD);
        }
    }

    @Test
    @DisplayName("TC_597_001 ResultSet getFetchSize returns fetch size")
    void tc597001GetFetchSize() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            assertThat(rs.getFetchSize()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    @DisplayName("TC_598_001 ResultSet getConcurrency returns concurrency mode")
    void tc598001GetConcurrency() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            assertThat(rs.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        }
    }

    @Test
    @DisplayName("TC_599_001 ResultSet getType returns result set type")
    void tc599001GetType() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            assertThat(rs.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
        }
    }

    @Test
    @DisplayName("TC_600_001 ResultSet setFetchSize applies fetch size")
    void tc600001SetFetchSize() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            rs.setFetchSize(20);
            assertThat(rs.getFetchSize()).isEqualTo(20);
        }
    }

    @Test
    @DisplayName("TC_601_001 ResultSet isAfterLast reports end-of-result state")
    void tc601001IsAfterLast() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery("select 1 as c1 from dual union all select 2 from dual")) {
            while (rs.next()) {
                // advance to end
            }
            assertThat(rs.isAfterLast()).isTrue();
        }
    }

    @Test
    @DisplayName("TC_602_001 ResultSet isBeforeFirst reports initial cursor state")
    void tc602001IsBeforeFirst() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            assertThat(rs.isBeforeFirst()).isTrue();
        }
    }

    @Test
    @DisplayName("TC_603_001 ResultSet isFirst reports the first row")
    void tc603001IsFirst() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.isFirst()).isTrue();
        }
    }

    @Test
    @DisplayName("TC_604_001 ResultSet isLast reports the last row")
    void tc604001IsLast() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery("select user_id() from dual")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.isLast()).isTrue();
        }
    }

    @Test
    @DisplayName("TC_605_001 ResultSet isClosed reports close state")
    void tc605001IsClosed() throws Exception {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select user_id() from dual");
        rs.close();
        stmt.close();

        assertThat(rs.isClosed()).isTrue();
    }

    @Test
    @DisplayName("TC_606_001 ResultSetMetaData getCatalogName returns catalog information")
    void tc606001GetCatalogName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_id() as uid from dual")) {
            assertThat(rs.getMetaData().getCatalogName(1)).isNotNull();
        }
    }

    @Test
    @DisplayName("TC_607_001 ResultSetMetaData getColumnClassName returns Java class name")
    void tc607001GetColumnClassName() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select 1 as c1 from dual")) {
            assertThat(rs.getMetaData().getColumnClassName(1)).isNotBlank();
        }
    }

    @Test
    @DisplayName("TC_608_001 ResultSetMetaData getColumnCount returns column count")
    void tc608001GetColumnCount() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_id() as uid, user_name() as uname from dual")) {
            assertThat(rs.getMetaData().getColumnCount()).isEqualTo(2);
        }
    }

    @Test
    @DisplayName("TC_609_001 ResultSetMetaData getColumnDisplaySize returns display width")
    void tc609001GetColumnDisplaySize() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_name() as uname from dual")) {
            assertThat(rs.getMetaData().getColumnDisplaySize(1)).isGreaterThan(0);
        }
    }

    @Test
    @DisplayName("TC_610_001 ResultSetMetaData getColumnLabel returns aliases")
    void tc610001GetColumnLabel() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_id() as uid from dual")) {
            assertThat(rs.getMetaData().getColumnLabel(1)).isEqualToIgnoringCase("UID");
        }
    }

    @Test
    @DisplayName("TC_611_001 ResultSetMetaData getColumnType returns SQL type")
    void tc611001GetColumnType() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_name() as uname from dual")) {
            assertThat(rs.getMetaData().getColumnType(1)).isNotZero();
        }
    }

    @Test
    @DisplayName("TC_612_001 ResultSetMetaData getColumnTypeName returns type name")
    void tc612001GetColumnTypeName() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_TYPE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(20))");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().getColumnTypeName(1)).containsIgnoringCase("CHAR");
        }
    }

    @Test
    @DisplayName("TC_613_001 ResultSetMetaData getPrecision returns declared precision")
    void tc613001GetPrecision() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_PREC");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(20))");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().getPrecision(1)).isEqualTo(20);
        }
    }

    @Test
    @DisplayName("TC_614_001 ResultSetMetaData getScale returns declared scale")
    void tc614001GetScale() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_SCALE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 numeric(9,2))");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().getScale(1)).isEqualTo(2);
        }
    }

    @Test
    @DisplayName("TC_615_001 ResultSetMetaData getSchemaName returns schema information")
    void tc615001GetSchemaName() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_SCHEMA");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().getSchemaName(1)).isNotNull();
        }
    }

    @Test
    @DisplayName("TC_616_001 ResultSetMetaData getTableName returns source table")
    void tc616001GetTableName() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_TABLE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().getTableName(1)).isEqualToIgnoringCase(tableName);
        }
    }

    @Test
    @DisplayName("TC_617_001 ResultSetMetaData isAutoIncrement reports auto increment state")
    void tc617001IsAutoIncrement() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_AUTO");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isAutoIncrement(1)).isFalse();
        }
    }

    @Test
    @DisplayName("TC_618_001 ResultSetMetaData isCaseSensitive reports text sensitivity")
    void tc618001IsCaseSensitive() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_CASE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 varchar(20))");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isCaseSensitive(1)).isTrue();
        }
    }

    @Test
    @DisplayName("TC_619_001 ResultSetMetaData isCurrency reports currency support")
    void tc619001IsCurrency() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_CURR");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isCurrency(1)).isFalse();
        }
    }

    @Test
    @DisplayName("TC_620_001 ResultSetMetaData isDefinitelyWritable reports definite writability")
    void tc620001IsDefinitelyWritable() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_DEFWR");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isDefinitelyWritable(1)).isIn(true, false);
        }
    }

    @Test
    @DisplayName("TC_621_001 ResultSetMetaData isNullable reports nullability")
    void tc621001IsNullable() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_NULL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int not null)");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isNullable(1)).isEqualTo(ResultSetMetaData.columnNoNulls);
        }
    }

    @Test
    @DisplayName("TC_622_001 ResultSetMetaData isReadOnly reports read-only status")
    void tc622001IsReadOnly() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_RDONLY");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isReadOnly(1)).isIn(true, false);
        }
    }

    @Test
    @DisplayName("TC_623_001 ResultSetMetaData isSearchable reports searchable columns")
    void tc623001IsSearchable() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_SEARCH");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isSearchable(1)).isTrue();
        }
    }

    @Test
    @DisplayName("TC_624_001 ResultSetMetaData isSigned reports signed numeric columns")
    void tc624001IsSigned() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_SIGNED");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isSigned(1)).isTrue();
        }
    }

    @Test
    @DisplayName("TC_625_001 ResultSetMetaData isWritable reports writable status")
    void tc625001IsWritable() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_RSMD_WRITE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
             ResultSet rs = stmt.executeQuery("select c1 from " + tableName)) {
            assertThat(rs.getMetaData().isWritable(1)).isIn(true, false);
        }
    }

    @Test
    @DisplayName("추가 작성 TC_572_NEG_001 ResultSet getCharacterStream throws for a missing column")
    void additionalTc572Neg001GetCharacterStreamThrowsForMissingColumn() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select user_name() as uname from dual")) {
            assertThat(rs.next()).isTrue();
            assertThatThrownBy(() -> rs.getCharacterStream("MISSING"))
                    .isInstanceOf(SQLException.class);
        }
    }

    private static String readAll(Reader reader) throws IOException {
        StringBuilder builder = new StringBuilder();
        char[] buffer = new char[128];
        int read;
        while ((read = reader.read(buffer)) != -1) {
            builder.append(buffer, 0, read);
        }
        return builder.toString();
    }

    private void insertBlob(String tableName, byte[] bytes) throws Exception {
        jdbc.begin(connection);
        try (var ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setBlob(1, new SerialBlob(bytes));
            ps.executeUpdate();
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }
    }
}
