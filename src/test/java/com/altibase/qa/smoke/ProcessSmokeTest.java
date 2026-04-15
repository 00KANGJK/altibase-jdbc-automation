package com.altibase.qa.smoke;

import com.altibase.qa.base.BaseCliTest;
import com.altibase.qa.infra.cli.CommandResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisabledOnOs(OS.WINDOWS)
class ProcessSmokeTest extends BaseCliTest {

    @Test
    @DisplayName("서버 작업 루트 경로를 생성할 수 있다")
    void createWorkDirectories() {
        CommandResult result = shell.execute(List.of(
                "mkdir",
                "-p",
                config.paths().workRoot(),
                config.paths().backupDir(),
                config.paths().datafileDir(),
                config.paths().exportDir(),
                config.paths().scriptDir(),
                config.paths().logCaptureDir()
        ));

        assertThat(result.isSuccess()).isTrue();
    }
}
