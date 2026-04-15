package com.altibase.qa.transaction;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.assertj.core.api.Assertions.assertThat;

class MultiSessionBasicTest extends BaseDbTest {

    @Test
    @DisplayName("두 개의 세션을 동시에 열어 기본 질의를 수행한다")
    void openTwoSessionsAndQuery() {
        Connection second = jdbc.open();
        try {
            QueryResult firstResult = jdbc.query(connection, "select db_name from v$database");
            QueryResult secondResult = jdbc.query(second, "select db_name from v$database");

            assertThat(firstResult.value(0, "DB_NAME")).isEqualTo("mydb");
            assertThat(secondResult.value(0, "DB_NAME")).isEqualTo("mydb");
        } finally {
            jdbc.closeQuietly(second);
        }
    }
}
