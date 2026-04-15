package com.altibase.qa.base;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(OS.WINDOWS)
public abstract class BaseCliTest extends BaseTest {
    @BeforeEach
    void setUpCli() {
        Assumptions.assumeTrue(config.execution().enableCliTests(), "CLI tests are disabled in config");
    }

    protected void requireDestructiveTestsEnabled() {
        Assumptions.assumeTrue(
                config.execution().enableDestructiveTests(),
                "Destructive tests are disabled in config"
        );
    }

    protected void requireRecoveryTestsEnabled() {
        Assumptions.assumeTrue(
                config.execution().enableRecoveryTests(),
                "Recovery tests are disabled in config"
        );
    }

    protected void requireReplicationTestsEnabled() {
        Assumptions.assumeTrue(
                config.execution().enableReplicationTests(),
                "Replication tests are disabled in config"
        );
    }
}
