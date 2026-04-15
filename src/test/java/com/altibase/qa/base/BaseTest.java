package com.altibase.qa.base;

import com.altibase.qa.config.ConfigLoader;
import com.altibase.qa.config.TestConfig;
import com.altibase.qa.infra.cli.IsqlHelper;
import com.altibase.qa.infra.cli.ShellHelper;
import com.altibase.qa.infra.jdbc.JdbcHelper;
import io.qameta.allure.Allure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayDeque;
import java.util.Deque;

public abstract class BaseTest {
    protected TestConfig config;
    protected JdbcHelper jdbc;
    protected ShellHelper shell;
    protected IsqlHelper isql;

    private final Deque<Runnable> cleanupTasks = new ArrayDeque<>();

    @BeforeEach
    void setUpBase(TestInfo testInfo) {
        config = ConfigLoader.load();
        jdbc = new JdbcHelper(config);
        shell = new ShellHelper(config);
        isql = new IsqlHelper(config, shell);
        Allure.parameter("testName", testInfo.getDisplayName());
        Allure.parameter("environment", config.env().name());
    }

    protected void registerCleanup(Runnable cleanup) {
        cleanupTasks.push(cleanup);
    }

    @AfterEach
    void tearDownBase() {
        while (!cleanupTasks.isEmpty()) {
            try {
                cleanupTasks.pop().run();
            } catch (Exception ignored) {
            }
        }
    }
}
