package com.altibase.qa.recovery;

import com.altibase.qa.base.BaseCliTest;
import com.altibase.qa.infra.cli.CommandResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;

@DisabledOnOs(OS.WINDOWS)
class ServerControlSmokeTest extends BaseCliTest {

    @Test
    @DisplayName("SYSDBA 경로로 데이터베이스 정보를 조회한다")
    void queryDatabaseAsSysdba() {
        requireRecoveryTestsEnabled();

        CommandResult result = isql.executeSqlAsSysdba("select db_name from v$database");

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.stdout()).contains("mydb");
    }
}
