package com.altibase.qa.replication;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;

@DisabledOnOs(OS.WINDOWS)
class ReplicationPropertySmokeTest extends BaseDbTest {

    @Test
    @DisplayName("복제 관련 속성을 조회한다")
    void queryReplicationProperties() {
        Assumptions.assumeTrue(
                config.execution().enableReplicationTests(),
                "Replication tests are disabled in config"
        );

        QueryResult result = jdbc.query(
                connection,
                "select name, value1 from v$property where name like 'REPLICATION_%'"
        );

        assertThat(result.size()).isGreaterThan(0);
    }
}
