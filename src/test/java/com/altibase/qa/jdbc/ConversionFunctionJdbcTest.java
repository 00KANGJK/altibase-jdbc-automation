package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConversionFunctionJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_181_001 ASCIISTR converts non-ASCII characters to UTF-16 escape sequences")
    void tc181001Asciistr() {
        assertQueryEquals("select asciistr('가A') from dual", "\\AC00A");
    }

    @Test
    @DisplayName("TC_182_001 BIN_TO_NUM converts binary strings to decimal numbers")
    void tc182001BinToNum() {
        assertQueryEquals("select bin_to_num('1010') from dual", "10");
    }

    @Test
    @DisplayName("TC_183_001 CONVERT converts characters from one charset to another")
    void tc183001Convert() {
        assertQueryEquals("select convert('ABC', 'US7ASCII', 'UTF8') from dual", "ABC");
    }

    @Test
    @DisplayName("TC_184_001 DATE_TO_UNIX converts DATE values to Unix time")
    void tc184001DateToUnix() {
        assertQueryEquals(
                "select date_to_unix(to_date('1970-01-02 00:00:00','YYYY-MM-DD HH24:MI:SS')) from dual",
                "86400"
        );
    }

    @Test
    @DisplayName("TC_185_001 HEX_ENCODE converts ASCII strings to hex")
    void tc185001HexEncode() {
        assertQueryEquals("select hex_encode('AB') from dual", "4142");
    }

    @Test
    @DisplayName("TC_186_001 HEX_DECODE converts hex strings to ASCII")
    void tc186001HexDecode() {
        assertQueryEquals("select hex_decode('4142') from dual", "AB");
    }

    @Test
    @DisplayName("TC_187_001 HEX_TO_NUM converts hex strings to decimal numbers")
    void tc187001HexToNum() {
        assertQueryEquals("select hex_to_num('FF') from dual", "255");
    }

    @Test
    @DisplayName("TC_188_001 OCT_TO_NUM converts octal strings to decimal numbers")
    void tc188001OctToNum() {
        assertQueryEquals("select oct_to_num('10') from dual", "8");
    }

    @Test
    @DisplayName("TC_189_001 RAW_TO_NUMERIC converts RAW values to NUMERIC")
    void tc189001RawToNumeric() {
        assertQueryEquals("select raw_to_numeric(to_raw('123')) from dual", "0");
    }

    @Test
    @DisplayName("TC_190_001 RAW_TO_INTEGER converts RAW values to INTEGER")
    void tc190001RawToInteger() {
        assertQueryEquals("select raw_to_integer(varbyte'01000000') from dual", "1");
    }

    @Test
    @DisplayName("TC_191_001 RAW_TO_VARCHAR converts RAW values back to VARCHAR")
    void tc191001RawToVarchar() {
        assertQueryEquals("select raw_to_varchar(to_raw('ABC')) from dual", "ABC");
    }

    @Test
    @DisplayName("TC_192_001 TO_BIN converts decimal values to binary strings")
    void tc192001ToBin() {
        assertQueryEquals("select to_bin(10) from dual", "1010");
    }

    @Test
    @DisplayName("TC_193_001 TO_CHAR(datetime) formats DATE values")
    void tc193001ToCharDatetime() {
        assertQueryEquals(
                "select to_char(to_date('2025-04-15 13:45:10','YYYY-MM-DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS') from dual",
                "2025/04/15 13:45:10"
        );
    }

    @Test
    @DisplayName("TC_194_001 TO_CHAR(number) formats numeric values")
    void tc194001ToCharNumber() {
        assertQueryEquals("select to_char(12345.67,'99999.99') from dual", " 12345.67");
    }

    @Test
    @DisplayName("TC_195_001 TO_DATE converts character strings to DATE values")
    void tc195001ToDate() {
        assertQueryEquals("select to_date('2025-04-15','YYYY-MM-DD') from dual", "2025-04-15 00:00:00.0");
    }

    @Test
    @DisplayName("TC_196_001 TO_HEX converts decimal values to hex strings")
    void tc196001ToHex() {
        assertQueryEquals("select to_hex(255) from dual", "FF");
    }

    @Test
    @DisplayName("TC_197_001 TO_INTERVAL converts numeric values to interval values")
    void tc197001ToInterval() {
        assertQueryEquals("select to_interval(3, 'DAY') from dual", "3 0:0:0.0");
    }

    @Test
    @DisplayName("TC_198_001 TO_NCHAR(character) converts character data to NCHAR")
    void tc198001ToNcharCharacter() {
        assertQueryEquals("select to_nchar('ABC') from dual", "ABC");
    }

    @Test
    @DisplayName("TC_199_001 TO_NCHAR(datetime) formats DATE values as NCHAR")
    void tc199001ToNcharDatetime() {
        assertQueryEquals(
                "select to_nchar(to_date('2025-04-15','YYYY-MM-DD'),'YYYY-MM-DD') from dual",
                "2025-04-15"
        );
    }

    @Test
    @DisplayName("TC_200_001 TO_NCHAR(number) converts numbers to NCHAR")
    void tc200001ToNcharNumber() {
        assertQueryEquals("select to_nchar(12345) from dual", "12345");
    }

    @Test
    @DisplayName("TC_201_001 TO_NUMBER converts character strings to numeric values")
    void tc201001ToNumber() {
        assertQueryEquals("select to_number('12,345.67','99,999.99') from dual", "12345.67");
    }

    @Test
    @DisplayName("TC_202_001 TO_OCT converts decimal values to octal strings")
    void tc202001ToOct() {
        assertQueryEquals("select to_oct(8) from dual", "10");
    }

    @Test
    @DisplayName("TC_203_001 TO_RAW converts values to RAW")
    void tc203001ToRaw() {
        QueryResult result = jdbc.query(connection, "select to_raw('ABC') as raw_value from dual");
        byte[] bytes = (byte[]) result.value(0, "RAW_VALUE");

        assertThat(toHex(bytes)).isEqualTo("0300414243");
    }

    @Test
    @DisplayName("TC_204_001 UNISTR converts strings to the national character set")
    void tc204001Unistr() {
        assertQueryEquals("select unistr('ABC') from dual", "ABC");
    }

    @Test
    @DisplayName("TC_205_001 UNIX_TO_DATE converts Unix time to DATE")
    void tc205001UnixToDate() {
        assertQueryEquals("select unix_to_date(86400) from dual", "1970-01-02 00:00:00.0");
    }

    @Test
    @DisplayName("Additional negative case: TO_NUMBER rejects incompatible formats")
    void toNumberRejectsInvalidFormats() {
        assertThatThrownBy(() ->
                jdbc.queryForString(connection, "select to_number('ABC','99') from dual"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Additional negative case: TO_DATE rejects invalid dates")
    void toDateRejectsInvalidDates() {
        assertThatThrownBy(() ->
                jdbc.queryForString(connection, "select to_date('2025-02-30','YYYY-MM-DD') from dual"))
                .isInstanceOf(IllegalStateException.class);
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
