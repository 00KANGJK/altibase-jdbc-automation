package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import com.altibase.qa.support.SqlExceptionSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("SqlNoDataSourceInspection")
class JdbcDriverContractJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional contract case: transaction isolation changes require an explicit transaction")
    void changingTransactionIsolationRequiresExplicitTransaction() {
        assertThatThrownBy(() -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED))
                .satisfies(throwable -> assertThat(SqlExceptionSupport.requireSqlException(throwable).getMessage())
                        .containsIgnoringCase("autocommit mode"));
    }

    @Test
    @DisplayName("Additional contract case: BLOB writes fail in autocommit mode")
    void blobWritesFailInAutocommitMode() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_BLOB_AUTOCOMMIT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 blob)");

        Blob blob = connection.createBlob();
        blob.setBytes(1, new byte[]{0x01, 0x02});

        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("insert into " + tableName + "(c1, c2) values(?, ?)")) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setBlob(2, blob);

            assertThatThrownBy(preparedStatement::executeUpdate)
                    .satisfies(throwable -> assertThat(SqlExceptionSupport.requireSqlException(throwable).getMessage())
                            .containsIgnoringCase("autocommit mode"));
        }
    }

    @Test
    @DisplayName("Additional contract case: CLOB writes fail in autocommit mode")
    void clobWritesFailInAutocommitMode() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_CLOB_AUTOCOMMIT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer primary key, c2 clob)");

        Clob clob = connection.createClob();
        clob.setString(1, "altibase");

        try (PreparedStatement preparedStatement =
                     connection.prepareStatement("insert into " + tableName + "(c1, c2) values(?, ?)")) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setClob(2, clob);

            assertThatThrownBy(preparedStatement::executeUpdate)
                    .satisfies(throwable -> assertThat(SqlExceptionSupport.requireSqlException(throwable).getMessage())
                            .containsIgnoringCase("autocommit mode"));
        }
    }
}
