package com.altibase.qa.base;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assumptions;

import java.sql.Connection;

public abstract class BaseDbTest extends BaseTest {
    protected Connection connection;

    @BeforeEach
    void setUpDb() {
        Assumptions.assumeTrue(config.execution().enableDbTests(), "DB tests are disabled in config");
        connection = jdbc.open();
    }

    @AfterEach
    void tearDownDb() {
        jdbc.closeQuietly(connection);
    }
}
