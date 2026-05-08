package com.altibase.qa.base;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;

public abstract class BaseDbTest extends BaseTest {
    protected Connection connection;

    @BeforeEach
    void setUpDb() {
        Assumptions.assumeTrue(config.execution().enableDbTests(), "DB tests are disabled in config");
        connection = jdbc.open();
    }

    @Override
    protected void tearDownResources() {
        jdbc.closeQuietly(connection);
    }
}
