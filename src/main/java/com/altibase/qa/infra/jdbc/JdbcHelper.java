package com.altibase.qa.infra.jdbc;

import com.altibase.qa.config.TestConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JdbcHelper {
    private final TestConfig config;

    public JdbcHelper(TestConfig config) {
        this.config = config;
    }

    public void loadDriver() {
        try {
            Class.forName(config.db().driverClass());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load JDBC driver: " + config.db().driverClass(), e);
        }
    }

    public Connection open() {
        loadDriver();
        try {
            Connection connection = DriverManager.getConnection(
                    config.db().jdbcUrl(),
                    config.db().user(),
                    config.db().password()
            );
            connection.setAutoCommit(true);
            return connection;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open JDBC connection", e);
        }
    }

    public Connection open(String user, String password) {
        loadDriver();
        try {
            Connection connection = DriverManager.getConnection(
                    config.db().jdbcUrl(),
                    user,
                    password
            );
            connection.setAutoCommit(true);
            return connection;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to open JDBC connection for user: " + user, e);
        }
    }

    public int executeUpdate(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(config.timeouts().querySeconds());
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("SQL update failed: " + sql, e);
        }
    }

    public QueryResult query(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(config.timeouts().querySeconds());
            try (ResultSet rs = statement.executeQuery(sql)) {
                return map(rs);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQL query failed: " + sql, e);
        }
    }

    public String queryForString(Connection connection, String sql) {
        QueryResult result = query(connection, sql);
        if (result.rows().isEmpty()) {
            return null;
        }
        String firstColumn = result.columns().isEmpty() ? null : result.columns().get(0);
        if (firstColumn == null) {
            return null;
        }
        Object value = result.rows().get(0).get(firstColumn);
        return value == null ? null : value.toString();
    }

    public boolean exists(Connection connection, String sql) {
        return !query(connection, sql).rows().isEmpty();
    }

    public PreparedStatement prepare(Connection connection, String sql) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setQueryTimeout(config.timeouts().querySeconds());
            return preparedStatement;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to prepare SQL: " + sql, e);
        }
    }

    public void begin(Connection connection) {
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to disable auto-commit", e);
        }
    }

    public void commit(Connection connection) {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("Commit failed", e);
        }
    }

    public void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new IllegalStateException("Rollback failed", e);
        }
    }

    public void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException ignored) {
        }
    }

    private QueryResult map(ResultSet rs) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        List<String> columns = new ArrayList<>();
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            columns.add(metadata.getColumnLabel(index).toUpperCase());
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int index = 1; index <= metadata.getColumnCount(); index++) {
                row.put(metadata.getColumnLabel(index).toUpperCase(), rs.getObject(index));
            }
            rows.add(row);
        }
        return new QueryResult(columns, rows);
    }
}
