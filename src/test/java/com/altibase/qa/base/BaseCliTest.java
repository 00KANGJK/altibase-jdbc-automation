package com.altibase.qa.base;

import com.altibase.qa.support.FeatureProbe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(OS.WINDOWS)
public abstract class BaseCliTest extends BaseTest {
    @BeforeEach
    void setUpCli() {
        FeatureProbe.assumeCliUtilitiesEnabled(config);
    }

    protected void requireDestructiveTestsEnabled() {
        FeatureProbe.assumeServerLifecycleEnabled(config);
    }

    protected void requireRecoveryTestsEnabled() {
        FeatureProbe.assumeRecoveryEnabled(config);
    }

    protected void requireReplicationTestsEnabled() {
        FeatureProbe.assumeReplicationSuiteEnabled(config);
    }
}
