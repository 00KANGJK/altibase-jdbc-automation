package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Driver;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class DriverJdbcTest extends BaseTest {

    @Test
    @DisplayName("TC_441_001 Driver acceptsURL reports valid JDBC URLs")
    void tc441001AcceptsUrl() throws Exception {
        Driver driver = new Altibase.jdbc.driver.AltibaseDriver();

        assertThat(driver.acceptsURL(config.db().jdbcUrl())).isTrue();
    }

    @Test
    @DisplayName("TC_443_001 Driver getMajorVersion returns the driver major version")
    void tc443001GetMajorVersion() {
        Driver driver = new Altibase.jdbc.driver.AltibaseDriver();

        assertThat(driver.getMajorVersion()).isEqualTo(7);
    }

    @Test
    @DisplayName("TC_444_001 Driver getMinorVersion returns the driver minor version")
    void tc444001GetMinorVersion() {
        Driver driver = new Altibase.jdbc.driver.AltibaseDriver();

        assertThat(driver.getMinorVersion()).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_445_001 Driver getPropertyInfo returns supported connection properties")
    void tc445001GetPropertyInfo() throws Exception {
        Driver driver = new Altibase.jdbc.driver.AltibaseDriver();
        Properties properties = new Properties();
        properties.setProperty("user", config.db().user());
        properties.setProperty("password", config.db().password());

        assertThat(driver.getPropertyInfo(config.db().jdbcUrl(), properties)).isNotEmpty();
    }

    @Test
    @DisplayName("TC_446_001 Driver jdbcCompliant reports JDBC compliance support")
    void tc446001JdbcCompliant() {
        Driver driver = new Altibase.jdbc.driver.AltibaseDriver();

        assertThat(driver.jdbcCompliant()).isFalse();
    }
}
