import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SignedIntegerBoundaryJdbcRepro {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java SignedIntegerBoundaryJdbcRepro <jdbcUrl> <user> <password>");
            System.exit(1);
        }

        String jdbcUrl = args[0];
        String user = args[1];
        String password = args[2];

        Class.forName("Altibase.jdbc.driver.AltibaseDriver");

        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
            connection.setAutoCommit(true);

            printVersion(connection);
            System.out.println();

            runForType(
                    connection,
                    "SMALLINT",
                    "smallint",
                    "-32768",
                    "-32767",
                    "32767"
            );

            runForType(
                    connection,
                    "INTEGER",
                    "integer",
                    "-2147483648",
                    "-2147483647",
                    "2147483647"
            );

            runForType(
                    connection,
                    "BIGINT",
                    "bigint",
                    "-9223372036854775808",
                    "-9223372036854775807",
                    "9223372036854775807"
            );
        }
    }

    private static void printVersion(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select product_version, product_time from v$version")) {
            if (resultSet.next()) {
                System.out.println("PRODUCT_VERSION = " + resultSet.getString(1));
                System.out.println("PRODUCT_TIME    = " + resultSet.getString(2));
            }
        }
    }

    private static void runForType(
            Connection connection,
            String label,
            String typeName,
            String minLiteral,
            String minPlusOneLiteral,
            String maxLiteral
    ) throws SQLException {
        System.out.println("[" + label + "]");

        runLiteralCase(connection, typeName, "LITERAL_MIN", minLiteral);
        runLiteralCase(connection, typeName, "LITERAL_MIN_PLUS_1", minPlusOneLiteral);
        runBoundCase(connection, typeName, "BIND_MIN", minLiteral);
        runBoundCase(connection, typeName, "BIND_MIN_PLUS_1", minPlusOneLiteral);
        runBoundCase(connection, typeName, "BIND_MAX", maxLiteral);

        System.out.println();
    }

    private static void runLiteralCase(Connection connection, String typeName, String caseName, String literalValue) throws SQLException {
        String tableName = tableName(typeName, caseName);
        recreateTable(connection, tableName, typeName);

        try {
            String sql = "insert into " + tableName + " values(" + literalValue + ")";
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql);
                System.out.println(caseName + " => SUCCESS, stored=" + selectSingleValue(connection, tableName));
            } catch (SQLException exception) {
                System.out.println(caseName + " => ERROR, message=" + singleLine(exception.getMessage()));
            }
        } finally {
            dropTableQuietly(connection, tableName);
        }
    }

    private static void runBoundCase(Connection connection, String typeName, String caseName, String value) throws SQLException {
        String tableName = tableName(typeName, caseName);
        recreateTable(connection, tableName, typeName);

        try {
            try (PreparedStatement preparedStatement =
                         connection.prepareStatement("insert into " + tableName + "(c1) values(?)")) {
                bind(preparedStatement, typeName, value);
                preparedStatement.executeUpdate();
                System.out.println(caseName + " => SUCCESS, stored=" + selectSingleValue(connection, tableName));
            } catch (SQLException exception) {
                System.out.println(caseName + " => ERROR, message=" + singleLine(exception.getMessage()));
            }
        } finally {
            dropTableQuietly(connection, tableName);
        }
    }

    private static void bind(PreparedStatement preparedStatement, String typeName, String value) throws SQLException {
        switch (typeName.toLowerCase()) {
            case "smallint":
                preparedStatement.setShort(1, Short.parseShort(value));
                break;
            case "integer":
                preparedStatement.setInt(1, Integer.parseInt(value));
                break;
            case "bigint":
                preparedStatement.setLong(1, Long.parseLong(value));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + typeName);
        }
    }

    private static void recreateTable(Connection connection, String tableName, String typeName) throws SQLException {
        dropTableQuietly(connection, tableName);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("create table " + tableName + "(c1 " + typeName + ")");
        }
    }

    private static void dropTableQuietly(Connection connection, String tableName) {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop table " + tableName + " cascade");
        } catch (SQLException ignored) {
        }
    }

    private static String selectSingleValue(Connection connection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select c1 from " + tableName)) {
            if (!resultSet.next()) {
                return "<NO ROW>";
            }
            String value = resultSet.getString(1);
            return value == null ? "<NULL>" : value;
        }
    }

    private static String tableName(String typeName, String caseName) {
        return ("QA_JDBC_" + typeName + "_" + caseName).toUpperCase();
    }

    private static String singleLine(String message) {
        if (message == null) {
            return "<NO MESSAGE>";
        }
        return message.replace('\n', ' ').replace('\r', ' ').trim();
    }
}
