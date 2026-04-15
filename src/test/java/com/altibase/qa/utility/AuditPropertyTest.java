package com.altibase.qa.utility;

import com.altibase.qa.base.BaseCliTest;
import com.altibase.qa.infra.cli.CommandResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;

@DisabledOnOs(OS.WINDOWS)
class AuditPropertyTest extends BaseCliTest {

    @Test
    @DisplayName("iSQL로 감사 관련 속성을 조회한다")
    void queryAuditProperties() {
        CommandResult result = isql.executeSql("""
                select name, value1
                  from v$property
                 where name like 'AUDIT_%'
                """);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.stdout()).contains("AUDIT_LOG_DIR");
        assertThat(result.stdout()).contains("AUDIT_OUTPUT_METHOD");
    }
}
