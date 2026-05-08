package com.altibase.qa.recovery;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import com.altibase.qa.support.FeatureProbe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecoveryJdbcSmokeTest extends BaseDbTest {

    @Test
    @DisplayName("ALTER SYSTEM CHECKPOINT can be executed through JDBC when recovery tests are enabled")
    void alterSystemCheckpointSucceeds() {
        FeatureProbe.assumeRecoveryEnabled(config);

        QueryResult before = jdbc.query(connection, "select count(*) as log_count from v$log");

        jdbc.executeUpdate(connection, "alter system checkpoint");

        QueryResult after = jdbc.query(connection, "select count(*) as log_count from v$log");

        assertThat(before.size()).isEqualTo(1);
        assertThat(after.size()).isEqualTo(1);
        assertThat(String.valueOf(before.value(0, "LOG_COUNT")))
                .isEqualTo(String.valueOf(after.value(0, "LOG_COUNT")));
    }
}
