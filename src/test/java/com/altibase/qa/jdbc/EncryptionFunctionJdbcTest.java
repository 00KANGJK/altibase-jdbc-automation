package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import com.altibase.qa.support.SqlExceptionSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EncryptionFunctionJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_206_001 AESENCRYPT can store AES-encrypted strings")
    void tc206001AesEncrypt() {
        String tableName = createEncryptedTable("QA_AES_ENC");

        insertEncrypted(tableName, "aesencrypt(pkcs7pad16('ABC AES TEST'), 'WORRAPS1WORRAPS2')");

        assertStoredCipherText(tableName);
    }

    @Test
    @DisplayName("TC_206_002 AESDECRYPT can decrypt AES-encrypted strings")
    void tc206002AesDecrypt() {
        String tableName = createEncryptedTable("QA_AES_DEC");
        insertEncrypted(tableName, "aesencrypt(pkcs7pad16('ABC AES TEST'), 'WORRAPS1WORRAPS2')");

        assertQueryEquals(
                "select pkcs7unpad16(aesdecrypt(encrypted_string, 'WORRAPS1WORRAPS2')) from " + tableName,
                "ABC AES TEST"
        );
    }

    @Test
    @DisplayName("TC_207_001 DESENCRYPT can store DES-encrypted strings")
    void tc207001DesEncrypt() {
        String tableName = createEncryptedTable("QA_DES_ENC");

        insertEncrypted(tableName, "desencrypt(pkcs7pad16('ABC DES TEST'), 'altibase')");

        assertStoredCipherText(tableName);
    }

    @Test
    @DisplayName("TC_207_002 DESDECRYPT can decrypt DES-encrypted strings")
    void tc207002DesDecrypt() {
        String tableName = createEncryptedTable("QA_DES_DEC");
        insertEncrypted(tableName, "desencrypt(pkcs7pad16('ABC DES TEST'), 'altibase')");

        assertQueryEquals(
                "select pkcs7unpad16(desdecrypt(encrypted_string, 'altibase')) from " + tableName,
                "ABC DES TEST"
        );
    }

    @Test
    @DisplayName("TC_207_003 TDESENCRYPT can store 3DES-encrypted strings")
    void tc207003TripleDesEncrypt() {
        String tableName = createEncryptedTable("QA_TDES_ENC");

        insertEncrypted(tableName, "tdesencrypt('A4 ALTIBASE Corporation.', 'altibaselocation')");

        assertStoredCipherText(tableName);
    }

    @Test
    @DisplayName("TC_207_004 TDESDECRYPT can decrypt 3DES-encrypted strings")
    void tc207004TripleDesDecrypt() {
        String tableName = createEncryptedTable("QA_TDES_DEC");
        insertEncrypted(tableName, "tdesencrypt('A4 ALTIBASE Corporation.', 'altibaselocation')");

        assertQueryEquals(
                "select tdesdecrypt(encrypted_string, 'altibaselocation') from " + tableName,
                "A4 ALTIBASE Corporation."
        );
    }

    @Test
    @DisplayName("TC_363_001 AES-encrypted text can be stored in a table and decrypted back")
    void tc363001StoreAndDecryptAesText() {
        String tableName = createEncryptedTable("QA_AES_TABLE");
        insertEncrypted(tableName, "aesencrypt(pkcs7pad16('TABLE AES TEXT'), 'WORRAPS1WORRAPS2')");

        assertQueryEquals(
                "select pkcs7unpad16(aesdecrypt(encrypted_string, 'WORRAPS1WORRAPS2')) from " + tableName,
                "TABLE AES TEXT"
        );
    }

    @Test
    @DisplayName("TC_363_002 AES-encrypted text can be inserted into a table")
    void tc363002InsertEncryptedAesText() {
        String tableName = createEncryptedTable("QA_AES_INSERT");

        insertEncrypted(tableName, "aesencrypt(pkcs7pad16('ABC AES TEST'), 'WORRAPS1WORRAPS2')");

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName)).isEqualTo("1");
        assertStoredCipherText(tableName);
    }

    @Test
    @DisplayName("TC_363_003 PKCS7PAD16 and PKCS7UNPAD16 support AES round-trips for non-block-sized strings")
    void tc363003Pkcs7PadAndUnpad() {
        assertQueryEquals(
                "select pkcs7unpad16(aesdecrypt(aesencrypt(pkcs7pad16('PADDED AES TEXT'), 'WORRAPS1WORRAPS2'), 'WORRAPS1WORRAPS2')) from dual",
                "PADDED AES TEXT"
        );
    }

    @Test
    @DisplayName("TC_363_004 PKCS7PAD16 values can be AES-encrypted and stored")
    void tc363004StorePkcs7PaddedAesText() {
        String tableName = createEncryptedTable("QA_AES_PAD_STORE");

        insertEncrypted(tableName, "aesencrypt(pkcs7pad16('PKCS7 AES TEXT'), 'WORRAPS1WORRAPS2')");

        assertStoredCipherText(tableName);
    }

    @Test
    @DisplayName("TC_363_005 PKCS7UNPAD16 restores AES-encrypted table data to the original string")
    void tc363005UnpadStoredAesText() {
        String tableName = createEncryptedTable("QA_AES_UNPAD");
        insertEncrypted(tableName, "aesencrypt(pkcs7pad16('UNPAD AES TEXT'), 'WORRAPS1WORRAPS2')");

        assertQueryEquals(
                "select pkcs7unpad16(aesdecrypt(encrypted_string, 'WORRAPS1WORRAPS2')) from " + tableName,
                "UNPAD AES TEXT"
        );
    }

    @Test
    @DisplayName("TC_364_001 DES-encrypted text can be stored in a table and decrypted back")
    void tc364001StoreAndDecryptDesText() {
        String tableName = createEncryptedTable("QA_DES_TABLE");
        insertEncrypted(tableName, "desencrypt(pkcs7pad16('TABLE DES TEXT'), 'altibase')");

        assertQueryEquals(
                "select pkcs7unpad16(desdecrypt(encrypted_string, 'altibase')) from " + tableName,
                "TABLE DES TEXT"
        );
    }

    @Test
    @DisplayName("TC_364_002 DES-encrypted text can be decrypted with the same key that was used to encrypt it")
    void tc364002DecryptDesTextWithSameKey() {
        String tableName = createEncryptedTable("QA_DES_SAME_KEY");
        insertEncrypted(tableName, "desencrypt(pkcs7pad16('SAME KEY DES TEXT'), 'altibase')");

        assertQueryEquals(
                "select pkcs7unpad16(desdecrypt(encrypted_string, 'altibase')) from " + tableName,
                "SAME KEY DES TEXT"
        );
    }

    @Test
    @DisplayName("Additional boundary case: AES ciphertext differs from plaintext and decrypts with the original key")
    void aesCipherTextDiffersFromPlaintextAndDecryptsWithOriginalKey() {
        String tableName = createEncryptedTable("QA_AES_CIPHER");
        String plainText = "SENSITIVE AES TEXT";
        insertEncrypted(tableName, "aesencrypt(pkcs7pad16('" + plainText + "'), 'WORRAPS1WORRAPS2')");

        assertCipherTextIsStoredAndNotPlainText(tableName, plainText);
        assertQueryEquals(
                "select pkcs7unpad16(aesdecrypt(encrypted_string, 'WORRAPS1WORRAPS2')) from " + tableName,
                plainText
        );
    }

    @Test
    @DisplayName("Additional boundary case: DES ciphertext differs from plaintext and decrypts with the original key")
    void desCipherTextDiffersFromPlaintextAndDecryptsWithOriginalKey() {
        String tableName = createEncryptedTable("QA_DES_CIPHER");
        String plainText = "SENSITIVE DES TEXT";
        insertEncrypted(tableName, "desencrypt(pkcs7pad16('" + plainText + "'), 'altibase')");

        assertCipherTextIsStoredAndNotPlainText(tableName, plainText);
        assertQueryEquals(
                "select pkcs7unpad16(desdecrypt(encrypted_string, 'altibase')) from " + tableName,
                plainText
        );
    }

    @Test
    @DisplayName("Additional boundary case: PKCS7PAD16 supports exact 16-byte block AES round-trips")
    void pkcs7Pad16SupportsExactBlockBoundaryRoundTrip() {
        assertQueryEquals(
                "select pkcs7unpad16(aesdecrypt(aesencrypt(pkcs7pad16('1234567890ABCDEF'), 'WORRAPS1WORRAPS2'), 'WORRAPS1WORRAPS2')) from dual",
                "1234567890ABCDEF"
        );
    }

    @Test
    @DisplayName("Additional negative case: AES decrypt with a wrong key cannot recover the plaintext")
    void aesDecryptWithWrongKeyDoesNotRecoverPlaintext() {
        String tableName = createEncryptedTable("QA_AES_WRONG_KEY");
        String plainText = "WRONG KEY AES TEXT";
        insertEncrypted(tableName, "aesencrypt(pkcs7pad16('" + plainText + "'), 'WORRAPS1WORRAPS2')");

        assertWrongKeyDoesNotReturnPlainText(
                "select pkcs7unpad16(aesdecrypt(encrypted_string, 'WORRAPS1WORRAPS9')) from " + tableName,
                plainText
        );
    }

    @Test
    @DisplayName("Additional negative case: DES decrypt with a wrong key cannot recover the plaintext")
    void desDecryptWithWrongKeyDoesNotRecoverPlaintext() {
        String tableName = createEncryptedTable("QA_DES_WRONG_KEY");
        String plainText = "WRONG KEY DES TEXT";
        insertEncrypted(tableName, "desencrypt(pkcs7pad16('" + plainText + "'), 'altibase')");

        assertWrongKeyDoesNotReturnPlainText(
                "select pkcs7unpad16(desdecrypt(encrypted_string, 'wrongkey')) from " + tableName,
                plainText
        );
    }

    @Test
    @DisplayName("Additional negative case: AESENCRYPT rejects unpadded input with an incompatible key length")
    void aesEncryptRejectsInvalidLengthInputs() {
        String tableName = createEncryptedTable("QA_AES_INVALID");

        assertThatThrownBy(() ->
                insertEncrypted(tableName, "aesencrypt('A4 ALTIBASE Corporation.', 'altibase')"))
                .isInstanceOf(IllegalStateException.class);
    }

    private String createEncryptedTable(String prefix) {
        String tableName = DbTestSupport.uniqueName(prefix);
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));
        jdbc.executeUpdate(connection, "create table " + tableName + "(encrypted_string varchar(200))");
        return tableName;
    }

    private void insertEncrypted(String tableName, String encryptionExpression) {
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(" + encryptionExpression + ")");
    }

    private void assertStoredCipherText(String tableName) {
        assertThat(jdbc.queryForString(connection,
                "select case when encrypted_string is null then 'N' else 'Y' end from " + tableName)).isEqualTo("Y");
    }

    private void assertCipherTextIsStoredAndNotPlainText(String tableName, String plainText) {
        assertThat(jdbc.queryForString(
                connection,
                "select case when encrypted_string is not null and encrypted_string <> '" + plainText + "' then 'Y' else 'N' end from " + tableName
        )).isEqualTo("Y");
    }

    private void assertWrongKeyDoesNotReturnPlainText(String sql, String plainText) {
        try {
            assertThat(jdbc.queryForString(connection, sql)).isNotEqualTo(plainText);
        } catch (IllegalStateException exception) {
            assertThat((Object) SqlExceptionSupport.findSqlException(exception)).isNotNull();
        }
    }

    private void assertQueryEquals(String sql, String expected) {
        assertThat(jdbc.queryForString(connection, sql)).isEqualTo(expected);
    }
}
