package com.altibase.qa.smoke;

import com.altibase.qa.base.BaseCliTest;
import com.altibase.qa.infra.cli.CommandResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;

@DisabledOnOs(OS.WINDOWS)
class IsqlSmokeTest extends BaseCliTest {

    @Test
    @DisplayName("iSQL로 데이터베이스 이름을 조회한다")
    void queryDatabaseNameWithIsql() {
        CommandResult result = isql.executeSql("select db_name from v$database");

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.stdout()).contains("mydb");
    }
}
