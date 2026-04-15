package com.altibase.qa.smoke;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.infra.jdbc.QueryResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BasicQuerySmokeTest extends BaseDbTest {

    @Test
    @DisplayName("기본 질의로 버전 정보를 조회한다")
    void queryVersion() {
        QueryResult result = jdbc.query(connection, "select product_version from v$version");
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.value(0, "PRODUCT_VERSION")).isNotNull();
    }

    @Test
    @DisplayName("기본 질의로 현재 날짜를 조회한다")
    void querySysdate() {
        QueryResult result = jdbc.query(connection, "select sysdate from dual");
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.rows().get(0).values()).isNotEmpty();
    }
}
