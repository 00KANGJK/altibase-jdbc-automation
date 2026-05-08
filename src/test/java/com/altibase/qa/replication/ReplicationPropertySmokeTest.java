package com.altibase.qa.replication;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import com.altibase.qa.support.FeatureProbe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicationPropertySmokeTest extends BaseDbTest {

    @Test
    @DisplayName("복제 관련 속성을 조회한다")
    void queryReplicationProperties() {
        FeatureProbe.assumeReplicationSuiteEnabled(config);

        QueryResult result = jdbc.query(
                connection,
                "select name, value1 from v$property where name like 'REPLICATION_%'"
        );

        assertThat(result.size()).isGreaterThan(0);
    }
}
