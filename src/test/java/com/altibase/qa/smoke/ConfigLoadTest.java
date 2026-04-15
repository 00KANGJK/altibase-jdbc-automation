package com.altibase.qa.smoke;

import com.altibase.qa.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigLoadTest extends BaseTest {

    @Test
    @DisplayName("환경 설정 파일을 로드한다")
    void loadConfig() {
        assertThat(config.db().host()).isEqualTo("210.104.181.224");
        assertThat(config.db().port()).isEqualTo(20300);
        assertThat(config.db().database()).isEqualTo("mydb");
        assertThat(config.client().isql()).contains("altibase-client-7.3.0/bin/isql");
        assertThat(config.server().server()).contains("altibase_home/bin/server");
    }
}
