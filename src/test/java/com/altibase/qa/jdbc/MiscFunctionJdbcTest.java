package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MiscFunctionJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_208_001 BASE64_DECODE decodes base64-encoded RAW values")
    void tc208001Base64Decode() {
        QueryResult result = jdbc.query(connection, "select base64_decode(base64_encode(to_raw('ABC'))) as value from dual");
        assertThat(toHex((byte[]) result.value(0, "VALUE"))).isEqualTo("0300414243");
    }

    @Test
    @DisplayName("TC_209_001 BASE64_DECODE_STR returns decoded data as hex text")
    void tc209001Base64DecodeStr() {
        assertQueryEquals("select base64_decode_str('QUJD') from dual", "414243");
    }

    @Test
    @DisplayName("TC_210_001 BASE64_ENCODE returns base64-encoded RAW text")
    void tc210001Base64Encode() {
        QueryResult result = jdbc.query(connection, "select base64_encode(to_raw('ABC')) as value from dual");
        assertThat(toHex((byte[]) result.value(0, "VALUE"))).isEqualTo("41774242516B4D3D");
    }

    @Test
    @DisplayName("TC_211_001 BASE64_ENCODE_STR converts hex text to base64")
    void tc211001Base64EncodeStr() {
        assertQueryEquals("select base64_encode_str('414243') from dual", "QUJD");
    }

    @Test
    @DisplayName("TC_212_001 BINARY_LENGTH returns the size of binary data")
    void tc212001BinaryLength() {
        assertQueryEquals("select binary_length(to_raw('ABC')) from dual", "5");
    }

    @Test
    @DisplayName("TC_213_001 CASE2 returns the first matching expression")
    void tc213001Case2() {
        assertQueryEquals("select case2(1=0, 'A', 2=2, 'B', 'C') from dual", "B");
    }

    @Test
    @DisplayName("TC_214_001 CASE WHEN returns the matching branch")
    void tc214001CaseWhen() {
        assertQueryEquals("select case 'B' when 'A' then 'X' when 'B' then 'Y' else 'Z' end from dual", "Y");
    }

    @Test
    @DisplayName("TC_215_001 COALESCE returns the first non-null expression")
    void tc215001Coalesce() {
        assertQueryEquals("select coalesce(null, null, 'ABC') from dual", "ABC");
    }

    @Test
    @DisplayName("TC_216_001 DECODE returns the matching expression or default")
    void tc216001Decode() {
        assertQueryEquals("select decode('B', 'A', 'X', 'B', 'Y', 'Z') from dual", "Y");
    }

    @Test
    @DisplayName("TC_217_001 DIGEST supports SHA-1")
    void tc217001DigestSha1() {
        assertQueryEquals("select digest('ABC', 'SHA1') from dual", "3C01BDBB26F358BAB27F267924AA2C9A03FCFDB8");
    }

    @Test
    @DisplayName("TC_217_002 DIGEST supports SHA-256")
    void tc217002DigestSha256() {
        assertQueryEquals(
                "select digest('ABC', 'SHA256') from dual",
                "B5D4045C3F466FA91FE2CC6ABE79232A1A57CDF104F7A26E716E0A1E2789DF78"
        );
    }

    @Test
    @DisplayName("TC_217_003 DIGEST supports SHA-512")
    void tc217003DigestSha512() {
        assertQueryEquals(
                "select digest('ABC', 'SHA512') from dual",
                "397118FDAC8D83AD98813C50759C85B8C47565D8268BF10DA483153B747A74743A58A90E85AA9F705CE6984FFC128DB567489817E4092D050D8A1CC596DDC119"
        );
    }

    @Test
    @DisplayName("TC_218_001 DUMP returns type, length, and content information")
    void tc218001Dump() {
        assertQueryEquals("select dump('ABC') from dual", "Type=CHAR(KSC5601) Length=5: 3,0,65,66,67");
    }

    @Test
    @DisplayName("TC_219_001 EMPTY_BLOB returns an initialized empty BLOB")
    void tc219001EmptyBlob() {
        assertQueryEquals("select case when empty_blob() is null then 'N' else 'Y' end from dual", "Y");
    }

    @Test
    @DisplayName("TC_220_001 EMPTY_CLOB returns an initialized empty CLOB")
    void tc220001EmptyClob() {
        assertQueryEquals("select case when empty_clob() is null then 'N' else 'Y' end from dual", "Y");
    }

    @Test
    @DisplayName("TC_221_001 GREATEST returns the largest value")
    void tc221001Greatest() {
        assertQueryEquals("select greatest('A','Z','M') from dual", "Z");
    }

    @Test
    @DisplayName("TC_222_001 GROUPING identifies rollup-generated nulls")
    void tc222001Grouping() {
        QueryResult result = jdbc.query(
                connection,
                "select dno, grouping(dno) as grp from " +
                        "(select 10 dno from dual union all select 20 from dual) t " +
                        "group by rollup(dno) order by dno"
        );

        assertThat(result.size()).isEqualTo(3);
        assertThat(((Number) result.value(0, "DNO")).intValue()).isEqualTo(10);
        assertThat(((Number) result.value(0, "GRP")).intValue()).isEqualTo(0);
        assertThat(result.value(2, "DNO")).isNull();
        assertThat(((Number) result.value(2, "GRP")).intValue()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_223_001 GROUPING_ID combines grouping flags into a decimal number")
    void tc223001GroupingId() {
        QueryResult result = jdbc.query(
                connection,
                "select dno, grouping_id(dno) as grp_id from " +
                        "(select 10 dno from dual union all select 20 from dual) t " +
                        "group by rollup(dno) order by dno"
        );

        assertThat(result.size()).isEqualTo(3);
        assertThat(((Number) result.value(0, "GRP_ID")).intValue()).isEqualTo(0);
        assertThat(((Number) result.value(2, "GRP_ID")).intValue()).isEqualTo(1);
    }

    @Test
    @DisplayName("TC_224_001 HOST_NAME returns the current host name")
    void tc224001HostName() {
        assertThat(jdbc.queryForString(connection, "select host_name() from dual")).isNotBlank();
    }

    @Test
    @DisplayName("TC_226_001 LNNVL returns true for FALSE or NULL conditions")
    void tc226001Lnnvl() {
        assertQueryEquals("select case when lnnvl(1=2) then 'T' else 'F' end from dual", "T");
    }

    @Test
    @DisplayName("TC_236_001 RAW_CONCAT concatenates RAW values")
    void tc236001RawConcat() {
        QueryResult result = jdbc.query(connection, "select raw_concat(to_raw('A'), to_raw('B')) as value from dual");
        assertThat(toHex((byte[]) result.value(0, "VALUE"))).isEqualTo("010041010042");
    }

    @Test
    @DisplayName("TC_237_001 RAW_SIZEOF returns the allocated RAW size")
    void tc237001RawSizeof() {
        assertQueryEquals("select raw_sizeof(to_raw('ABC')) from dual", "7");
    }

    @Test
    @DisplayName("TC_238_001 ROWNUM returns pseudo row numbers")
    void tc238001Rownum() {
        assertQueryEquals("select rownum from dual", "1");
    }

    @Test
    @DisplayName("TC_243_001 SUBRAW extracts a range of bytes from RAW data")
    void tc243001Subraw() {
        QueryResult result = jdbc.query(connection, "select subraw(to_raw('ABCDE'), 2, 2) as value from dual");
        assertThat(toHex((byte[]) result.value(0, "VALUE"))).isEqualTo("0041");
    }

    @Test
    @DisplayName("TC_244_001 SYS_CONNECT_BY_PATH builds a path string from root to node")
    void tc244001SysConnectByPath() {
        assertQueryEquals(
                "select sys_connect_by_path(name, '/') from " +
                        "(select 'A' name, 1 id, cast(null as integer) pid from dual " +
                        "union all select 'B', 2, 1 from dual) t " +
                        "start with pid is null connect by prior id = pid order siblings by id",
                "/A"
        );
    }

    @Test
    @DisplayName("TC_245_001 SYS_GUID_STR returns a 32-character GUID string")
    void tc245001SysGuidStr() {
        assertQueryEquals("select length(sys_guid_str()) from dual", "32");
    }

    @Test
    @DisplayName("TC_246_001 USER_LOCK_REQUEST requests a user lock and returns a status code")
    void tc246001UserLockRequest() {
        assertQueryEquals("select user_lock_request(1234) from dual", "0");
    }

    @Test
    @DisplayName("TC_247_001 USER_LOCK_RELEASE releases a user lock and returns a status code")
    void tc247001UserLockRelease() {
        jdbc.queryForString(connection, "select user_lock_request(1234) from dual");
        assertQueryEquals("select user_lock_release(1234) from dual", "0");
    }

    private void assertQueryEquals(String sql, String expected) {
        assertThat(jdbc.queryForString(connection, sql)).isEqualTo(expected);
    }

    private String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (byte value : bytes) {
            builder.append(String.format("%02X", value));
        }
        return builder.toString();
    }
}
