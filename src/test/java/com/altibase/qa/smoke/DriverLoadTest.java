package com.altibase.qa.smoke;

import com.altibase.qa.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class DriverLoadTest extends BaseTest {

    @Test
    @DisplayName("Altibase JDBC 드라이버를 로드한다")
    void loadDriver() {
        assertThatCode(() -> jdbc.loadDriver()).doesNotThrowAnyException();
    }
}
