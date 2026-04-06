package com.altibase.qa.base;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class DriverLoadTest {

    @Test
    @DisplayName("Altibase JDBC 드라이버 로딩 테스트")
    void testDriverLoading() {
        assertDoesNotThrow(() -> {
            // 1. 드라이버 클래스를 강제로 메모리에 로드 시도
            Class.forName("Altibase.jdbc.driver.AltibaseDriver");
            System.out.println("✅ Altibase 드라이버 로드 성공!");
        }, "드라이버 클래스를 찾을 수 없습니다. pom.xml 설정을 확인하세요.");
    }
}