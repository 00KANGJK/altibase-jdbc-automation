package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import javax.sql.rowset.serial.SerialBlob;

import static org.assertj.core.api.Assertions.assertThat;

class PreparedStatementJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_648_001 PreparedStatement addBatch registers rows")
    void tc648001AddBatch() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_BATCH");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1, c2) values(?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "A");
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setString(2, "B");
            ps.addBatch();

            assertThat(ps.executeBatch()).hasSize(2);
        }

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isEqualTo(2);
    }

    @Test
    @DisplayName("TC_649_001 PreparedStatement clearParameters clears bindings")
    void tc649001ClearParameters() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_CLEAR");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1, c2) values(?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "A");
            ps.clearParameters();
            ps.setInt(1, 2);
            ps.setString(2, "B");
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 2")).isEqualTo("B");
    }

    @Test
    @DisplayName("TC_650_001 PreparedStatement execute runs the statement")
    void tc650001Execute() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_EXEC");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1, c2) values(?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "A");
            assertThat(ps.execute()).isFalse();
        }

        assertThat(jdbc.query(connection, "select * from " + tableName).size()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_651_001 PreparedStatement executeQuery returns a result set")
    void tc651001ExecuteQuery() throws Exception {
        try (PreparedStatement ps = jdbc.prepare(connection, "select user_name() from dual");
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualToIgnoringCase("SYS");
        }
    }

    @Test
    @DisplayName("TC_652_001 PreparedStatement executeUpdate returns update count")
    void tc652001ExecuteUpdate() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_UPD");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1, c2) values(?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "A");
            assertThat(ps.executeUpdate()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("TC_653_001 PreparedStatement getMetaData returns metadata")
    void tc653001GetMetaData() throws Exception {
        try (PreparedStatement ps = jdbc.prepare(connection, "select user_id() as uid, user_name() as uname from dual")) {
            assertThat(ps.getMetaData()).isNotNull();
            assertThat(ps.getMetaData().getColumnCount()).isEqualTo(2);
        }
    }

    @Test
    @DisplayName("TC_654_001 PreparedStatement setBigDecimal binds decimal values")
    void tc654001SetBigDecimal() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_BIGDEC");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 numeric(10,2))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setBigDecimal(1, new BigDecimal("12.34"));
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select c1 from " + tableName)).startsWith("12.34");
    }

    @Test
    @DisplayName("TC_655_001 PreparedStatement setBinaryStream binds binary data")
    void tc655001SetBinaryStream() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_BINSTR");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 blob)");

        jdbc.begin(connection);
        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setBinaryStream(1, new ByteArrayInputStream(new byte[]{0x01, 0x02}));
            ps.executeUpdate();
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        try (PreparedStatement ps = jdbc.prepare(connection, "select c1 from " + tableName);
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            try (InputStream inputStream = rs.getBinaryStream(1)) {
                assertThat(inputStream.readAllBytes()).containsExactly((byte) 0x01, (byte) 0x02);
            }
        }
    }

    @Test
    @DisplayName("TC_656_001 PreparedStatement setBinaryStream with length binds binary data")
    void tc656001SetBinaryStreamWithLength() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_BINLEN");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 blob)");

        jdbc.begin(connection);
        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setBinaryStream(1, new ByteArrayInputStream(new byte[]{0x0A, 0x0B}), 2);
            ps.executeUpdate();
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        try (PreparedStatement ps = jdbc.prepare(connection, "select c1 from " + tableName);
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            try (InputStream inputStream = rs.getBinaryStream(1)) {
                assertThat(inputStream.readAllBytes()).containsExactly((byte) 0x0A, (byte) 0x0B);
            }
        }
    }

    @Test
    @DisplayName("TC_657_001 PreparedStatement setBlob with Blob binds blob data")
    void tc657001SetBlob() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_BLOB");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 blob)");

        jdbc.begin(connection);
        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setBlob(1, new SerialBlob(new byte[]{0x01, 0x02, 0x03}));
            ps.executeUpdate();
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        try (PreparedStatement ps = jdbc.prepare(connection, "select c1 from " + tableName);
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            try (InputStream inputStream = rs.getBinaryStream(1)) {
                assertThat(inputStream.readAllBytes()).containsExactly((byte) 0x01, (byte) 0x02, (byte) 0x03);
            }
        }
    }

    @Test
    @DisplayName("TC_658_001 PreparedStatement setBlob with InputStream binds blob data")
    void tc658001SetBlobWithInputStream() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_BLOBIS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 blob)");

        jdbc.begin(connection);
        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setBlob(1, new ByteArrayInputStream(new byte[]{0x0A, 0x0B}));
            ps.executeUpdate();
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        try (PreparedStatement ps = jdbc.prepare(connection, "select c1 from " + tableName);
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            try (InputStream inputStream = rs.getBinaryStream(1)) {
                assertThat(inputStream.readAllBytes()).containsExactly((byte) 0x0A, (byte) 0x0B);
            }
        }
    }

    @Test
    @DisplayName("TC_659_001 PreparedStatement setByte binds byte values")
    void tc659001SetByte() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_BYTE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 smallint)");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setByte(1, (byte) 7);
            ps.executeUpdate();
        }

        assertThat(Byte.parseByte(jdbc.queryForString(connection, "select c1 from " + tableName))).isEqualTo((byte) 7);
    }

    @Test
    @DisplayName("TC_660_001 PreparedStatement setBytes binds byte array values")
    void tc660001SetBytes() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_BYTES");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 byte(3))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setBytes(1, new byte[]{0x01, 0x23, 0x45});
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select to_char(c1) from " + tableName)).isEqualTo("012345");
    }

    @Test
    @DisplayName("TC_661_001 PreparedStatement setCharacterStream binds text data")
    void tc661001SetCharacterStream() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_CHARSTR");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 clob)");

        jdbc.begin(connection);
        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setCharacterStream(1, new StringReader("stream-text"));
            ps.executeUpdate();
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        try (PreparedStatement ps = jdbc.prepare(connection, "select c1 from " + tableName);
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            try (Reader reader = rs.getCharacterStream(1)) {
                assertThat(readAll(reader)).isEqualTo("stream-text");
            }
        }
    }

    @Test
    @DisplayName("TC_662_001 PreparedStatement setCharacterStream with length binds text data")
    void tc662001SetCharacterStreamWithLength() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_CHARLEN");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 clob)");

        jdbc.begin(connection);
        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setCharacterStream(1, new StringReader("reader-data"), 11);
            ps.executeUpdate();
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        try (PreparedStatement ps = jdbc.prepare(connection, "select c1 from " + tableName);
             ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            try (Reader reader = rs.getCharacterStream(1)) {
                assertThat(readAll(reader)).isEqualTo("reader-data");
            }
        }
    }

    @Test
    @DisplayName("TC_663_001 PreparedStatement setDate binds date values")
    void tc663001SetDate() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_DATE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 date)");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setDate(1, Date.valueOf("2024-01-02"));
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select to_char(c1, 'YYYY-MM-DD') from " + tableName)).isEqualTo("2024-01-02");
    }

    @Test
    @DisplayName("TC_666_001 PreparedStatement setInt binds integer values")
    void tc666001SetInt() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_INT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1, c2) values(?, ?)")) {
            ps.setInt(1, 77);
            ps.setString(2, "INT");
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select c1 from " + tableName + " where c2 = 'INT'")).isEqualTo("77");
    }

    @Test
    @DisplayName("TC_664_001 PreparedStatement setDouble binds double values")
    void tc664001SetDouble() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_DOUBLE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 double)");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setDouble(1, 12.5d);
            ps.executeUpdate();
        }

        assertThat(Double.parseDouble(jdbc.queryForString(connection, "select c1 from " + tableName))).isEqualTo(12.5d);
    }

    @Test
    @DisplayName("TC_665_001 PreparedStatement setFloat binds float values")
    void tc665001SetFloat() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_FLOAT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 float)");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setFloat(1, 9.5f);
            ps.executeUpdate();
        }

        assertThat(Float.parseFloat(jdbc.queryForString(connection, "select c1 from " + tableName))).isEqualTo(9.5f);
    }

    @Test
    @DisplayName("TC_668_001 PreparedStatement setNull binds null values")
    void tc668001SetNull() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_NULL");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1, c2) values(?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.VARCHAR);
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1")).isNull();
    }

    @Test
    @DisplayName("TC_667_001 PreparedStatement setLong binds long values")
    void tc667001SetLong() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_LONG");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 bigint)");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setLong(1, 123456789L);
            ps.executeUpdate();
        }

        assertThat(Long.parseLong(jdbc.queryForString(connection, "select c1 from " + tableName))).isEqualTo(123456789L);
    }

    @Test
    @DisplayName("TC_669_001 PreparedStatement setObject binds object values")
    void tc669001SetObject() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_OBJ");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1, c2) values(?, ?)")) {
            ps.setObject(1, 7);
            ps.setObject(2, "OBJ");
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 7")).isEqualTo("OBJ");
    }

    @Test
    @DisplayName("TC_670_001 PreparedStatement setShort binds short values")
    void tc670001SetShort() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_SHORT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 smallint)");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setShort(1, (short) 12);
            ps.executeUpdate();
        }

        assertThat(Short.parseShort(jdbc.queryForString(connection, "select c1 from " + tableName))).isEqualTo((short) 12);
    }

    @Test
    @DisplayName("TC_671_001 PreparedStatement setString binds string values")
    void tc671001SetString() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_STR");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1, c2) values(?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "ALTIBASE");
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select c2 from " + tableName + " where c1 = 1")).isEqualTo("ALTIBASE");
    }

    @Test
    @DisplayName("TC_672_001 PreparedStatement setTime binds time values")
    void tc672001SetTime() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_TIME");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 date)");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setTime(1, Time.valueOf("03:04:05"));
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select to_char(c1, 'HH24:MI:SS') from " + tableName)).isEqualTo("03:04:05");
    }

    @Test
    @DisplayName("TC_673_001 PreparedStatement setTimestamp binds timestamp values")
    void tc673001SetTimestamp() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_PS_TS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 date)");

        try (PreparedStatement ps = jdbc.prepare(connection, "insert into " + tableName + "(c1) values(?)")) {
            ps.setTimestamp(1, Timestamp.valueOf("2024-01-02 03:04:05"));
            ps.executeUpdate();
        }

        assertThat(jdbc.queryForString(connection, "select to_char(c1, 'YYYY-MM-DD HH24:MI:SS') from " + tableName)).isEqualTo("2024-01-02 03:04:05");
    }

    private static String readAll(Reader reader) throws Exception {
        StringBuilder builder = new StringBuilder();
        char[] buffer = new char[128];
        int read;
        while ((read = reader.read(buffer)) != -1) {
            builder.append(buffer, 0, read);
        }
        return builder.toString();
    }
}
