package com.altibase.qa.support;

import com.altibase.qa.infra.jdbc.JdbcHelper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;

public final class DbTestSupport {
    private DbTestSupport() {
    }

    public static String uniqueName(String prefix) {
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase(Locale.ROOT);
        return (prefix + "_" + suffix).toUpperCase(Locale.ROOT);
    }

    public static boolean tableExists(Connection connection, String schema, String tableName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getTables(null, schema, tableName.toUpperCase(Locale.ROOT), null)) {
                return resultSet.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to inspect table metadata for " + tableName, e);
        }
    }

    public static boolean viewExists(Connection connection, String schema, String viewName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getTables(null, schema, viewName.toUpperCase(Locale.ROOT), new String[]{"VIEW"})) {
                return resultSet.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to inspect view metadata for " + viewName, e);
        }
    }

    public static boolean columnExists(Connection connection, String schema, String tableName, String columnName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getColumns(
                    null,
                    schema,
                    tableName.toUpperCase(Locale.ROOT),
                    columnName.toUpperCase(Locale.ROOT)
            )) {
                return resultSet.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to inspect column metadata for " + tableName + "." + columnName, e);
        }
    }

    public static void dropTableQuietly(JdbcHelper jdbc, Connection connection, String tableName) {
        try {
            jdbc.executeUpdate(connection, "drop table " + tableName + " cascade");
        } catch (Exception ignored) {
        }
    }

    public static void dropViewQuietly(JdbcHelper jdbc, Connection connection, String viewName) {
        try {
            jdbc.executeUpdate(connection, "drop view " + viewName);
        } catch (Exception ignored) {
        }
    }

    public static void dropUserQuietly(JdbcHelper jdbc, Connection connection, String userName) {
        try {
            jdbc.executeUpdate(connection, "drop user " + userName + " cascade");
        } catch (Exception ignored) {
        }
    }

    public static void dropRoleQuietly(JdbcHelper jdbc, Connection connection, String roleName) {
        try {
            jdbc.executeUpdate(connection, "drop role " + roleName);
        } catch (Exception ignored) {
        }
    }

    public static void dropProcedureQuietly(JdbcHelper jdbc, Connection connection, String procedureName) {
        try {
            jdbc.executeUpdate(connection, "drop procedure " + procedureName);
        } catch (Exception ignored) {
        }
    }

    public static void dropFunctionQuietly(JdbcHelper jdbc, Connection connection, String functionName) {
        try {
            jdbc.executeUpdate(connection, "drop function " + functionName);
        } catch (Exception ignored) {
        }
    }

    public static void dropTriggerQuietly(JdbcHelper jdbc, Connection connection, String triggerName) {
        try {
            jdbc.executeUpdate(connection, "drop trigger " + triggerName);
        } catch (Exception ignored) {
        }
    }

    public static void dropSynonymQuietly(JdbcHelper jdbc, Connection connection, String synonymName) {
        try {
            jdbc.executeUpdate(connection, "drop synonym " + synonymName);
        } catch (Exception ignored) {
        }
    }

    public static void dropPublicSynonymQuietly(JdbcHelper jdbc, Connection connection, String synonymName) {
        try {
            jdbc.executeUpdate(connection, "drop public synonym " + synonymName);
        } catch (Exception ignored) {
        }
    }

    public static void dropTablespaceQuietly(JdbcHelper jdbc, Connection connection, String tablespaceName) {
        try {
            jdbc.executeUpdate(connection, "drop tablespace " + tablespaceName + " including contents");
        } catch (Exception ignored) {
        }
    }

    public static boolean userExists(JdbcHelper jdbc, Connection connection, String userName) {
        return jdbc.exists(
                connection,
                "select user_name from system_.sys_users_ where user_name = '" + userName.toUpperCase(Locale.ROOT) + "'"
        );
    }

    public static boolean roleExists(JdbcHelper jdbc, Connection connection, String roleName) {
        return jdbc.exists(
                connection,
                "select user_name from system_.sys_users_ where user_name = '" + roleName.toUpperCase(Locale.ROOT) + "'"
        );
    }
}
