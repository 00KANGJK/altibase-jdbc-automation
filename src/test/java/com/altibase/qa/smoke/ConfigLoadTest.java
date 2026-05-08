package com.altibase.qa.smoke;

import com.altibase.qa.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigLoadTest extends BaseTest {

    @Test
    @DisplayName("환경 설정 파일을 로드한다")
    void loadConfig() {
        assertThat(config.db().host()).isNotBlank();
        assertThat(config.db().port()).isGreaterThan(0);
        assertThat(config.db().database()).isNotBlank();
        assertThat(config.db().jdbcUrl()).startsWith("jdbc:Altibase://");
        assertThat(config.client().isql()).isNotBlank();
        assertThat(config.server().server()).isNotBlank();
        assertThat(config.paths().workRoot()).isNotBlank();
    }
}
