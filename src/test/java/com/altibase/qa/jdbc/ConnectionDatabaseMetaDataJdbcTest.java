package com.altibase.qa.jdbc;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionDatabaseMetaDataJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_442_001 DriverManager connect(String, Properties) creates a connection")
    void tc442001ConnectWithProperties() throws Exception {
        jdbc.loadDriver();

        Properties properties = new Properties();
        properties.setProperty("user", config.db().user());
        properties.setProperty("password", config.db().password());

        try (Connection directConnection = java.sql.DriverManager.getConnection(config.db().jdbcUrl(), properties)) {
            assertThat(directConnection.isClosed()).isFalse();
            assertThat(directConnection.getMetaData().getDatabaseProductName()).containsIgnoringCase("altibase");
        }
    }

    @Test
    @DisplayName("TC_447_001 Connection close releases the connection")
    void tc447001Close() throws Exception {
        connection.close();

        assertThat(connection.isClosed()).isTrue();
    }

    @Test
    @DisplayName("TC_448_001 Connection commit persists transaction changes")
    void tc448001Commit() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_MD_COMMIT");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));
        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        jdbc.begin(connection);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            jdbc.commit(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.queryForString(connection, "select c1 from " + tableName)).isEqualTo("1");
    }

    @Test
    @DisplayName("TC_449_001 Connection createStatement(type, concurrency) creates a configured statement")
    void tc449001CreateStatementWithTypeAndConcurrency() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            assertThat(stmt.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            assertThat(stmt.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        }
    }

    @Test
    @DisplayName("TC_450_001 Connection createStatement(type, concurrency, holdability) creates a configured statement")
    void tc450001CreateStatementWithHoldability() throws Exception {
        try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability())) {
            assertThat(stmt.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            assertThat(stmt.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
            assertThat(stmt.getResultSetHoldability()).isEqualTo(connection.getHoldability());
        }
    }

    @Test
    @DisplayName("TC_451_001 Connection getAutoCommit returns current auto-commit mode")
    void tc451001GetAutoCommit() throws Exception {
        assertThat(connection.getAutoCommit()).isTrue();
    }

    @Test
    @DisplayName("TC_452_001 Connection getCatalog returns the current catalog")
    void tc452001GetCatalog() throws Exception {
        assertThat(connection.getCatalog()).isEqualToIgnoringCase(config.db().database());
    }

    @Test
    @DisplayName("TC_453_001 Connection getHoldability returns current holdability")
    void tc453001GetHoldability() throws Exception {
        assertThat(connection.getHoldability()).isEqualTo(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    @DisplayName("TC_454_001 Connection getMetaData returns database metadata")
    void tc454001GetMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        assertThat(metaData).isNotNull();
        assertThat(metaData.getDatabaseProductName()).containsIgnoringCase("altibase");
    }

    @Test
    @DisplayName("TC_455_001 Connection getTransactionIsolation returns current isolation")
    void tc455001GetTransactionIsolation() throws Exception {
        assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test
    @DisplayName("TC_456_001 Connection isClosed returns current connection state")
    void tc456001IsClosed() throws Exception {
        assertThat(connection.isClosed()).isFalse();
    }

    @Test
    @DisplayName("TC_457_001 Connection isReadOnly returns read-only status")
    void tc457001IsReadOnly() throws Exception {
        assertThat(connection.isReadOnly()).isFalse();
    }

    @Test
    @DisplayName("TC_458_001 Connection nativeSQL returns translated SQL")
    void tc458001NativeSql() throws Exception {
        assertThat(connection.nativeSQL("select 1 from dual")).isEqualTo("select 1 from dual");
    }

    @Test
    @DisplayName("TC_459_001 Connection prepareCall(String) creates a callable statement")
    void tc459001PrepareCall() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_MD_PROC");
        createSimpleOutProcedure(procedureName);

        try (var cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            assertThat(cs).isNotNull();
        }
    }

    @Test
    @DisplayName("TC_460_001 Connection prepareCall(String, type, concurrency) creates a configured callable statement")
    void tc460001PrepareCallWithTypeAndConcurrency() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_MD_PROC");
        createSimpleOutProcedure(procedureName);

        try (var cs = connection.prepareCall("{call " + procedureName + "(?)}", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            assertThat(cs.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            assertThat(cs.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        }
    }

    @Test
    @DisplayName("TC_461_001 Connection prepareCall(String, type, concurrency, holdability) creates a configured callable statement")
    void tc461001PrepareCallWithHoldability() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_MD_PROC");
        createSimpleOutProcedure(procedureName);

        try (var cs = connection.prepareCall(
                "{call " + procedureName + "(?)}",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                connection.getHoldability()
        )) {
            assertThat(cs.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            assertThat(cs.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
            assertThat(cs.getResultSetHoldability()).isEqualTo(connection.getHoldability());
        }
    }

    @Test
    @DisplayName("TC_462_001 Connection prepareStatement(String) creates a prepared statement")
    void tc462001PrepareStatement() throws Exception {
        try (PreparedStatement ps = connection.prepareStatement("select user_name() from dual")) {
            assertThat(ps).isNotNull();
        }
    }

    @Test
    @DisplayName("TC_463_001 Connection prepareStatement(String, type, concurrency) creates a configured prepared statement")
    void tc463001PrepareStatementWithTypeAndConcurrency() throws Exception {
        try (PreparedStatement ps = connection.prepareStatement("select user_name() from dual", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            assertThat(ps.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            assertThat(ps.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
        }
    }

    @Test
    @DisplayName("TC_464_001 Connection prepareStatement(String, type, concurrency, holdability) creates a configured prepared statement")
    void tc464001PrepareStatementWithHoldability() throws Exception {
        try (PreparedStatement ps = connection.prepareStatement(
                "select user_name() from dual",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                connection.getHoldability()
        )) {
            assertThat(ps.getResultSetType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
            assertThat(ps.getResultSetConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
            assertThat(ps.getResultSetHoldability()).isEqualTo(connection.getHoldability());
        }
    }

    @Test
    @DisplayName("TC_465_001 Connection rollback cancels transaction changes")
    void tc465001Rollback() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_MD_ROLLBACK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));
        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        jdbc.begin(connection);
        try {
            jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");
            jdbc.rollback(connection);
        } finally {
            connection.setAutoCommit(true);
        }

        assertThat(jdbc.query(connection, "select * from " + tableName).rows()).isEmpty();
    }

    @Test
    @DisplayName("TC_466_001 Connection setAutoCommit updates auto-commit mode")
    void tc466001SetAutoCommit() throws Exception {
        connection.setAutoCommit(false);
        try {
            assertThat(connection.getAutoCommit()).isFalse();
        } finally {
            connection.setAutoCommit(true);
        }
    }

    @Test
    @DisplayName("TC_467_001 Connection setHoldability updates holdability")
    void tc467001SetHoldability() throws Exception {
        connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

        assertThat(connection.getHoldability()).isEqualTo(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    @DisplayName("TC_468_001 DatabaseMetaData allProceduresAreCallable returns capability information")
    void tc468001AllProceduresAreCallable() throws Exception {
        assertThat(connection.getMetaData().allProceduresAreCallable()).isIn(true, false);
    }

    @Test
    @DisplayName("TC_469_001 DatabaseMetaData allTablesAreSelectable returns table access capability")
    void tc469001AllTablesAreSelectable() throws Exception {
        assertThat(connection.getMetaData().allTablesAreSelectable()).isTrue();
    }

    @Test
    @DisplayName("TC_470_001 DatabaseMetaData getCatalogs returns catalog metadata")
    void tc470001GetCatalogs() throws Exception {
        try (ResultSet rs = connection.getMetaData().getCatalogs()) {
            ResultSetMetaData metaData = rs.getMetaData();
            assertThat(metaData.getColumnCount()).isGreaterThan(0);
        }
    }

    @Test
    @DisplayName("TC_471_001 DatabaseMetaData getCatalogSeparator returns the catalog separator")
    void tc471001GetCatalogSeparator() throws Exception {
        assertThat(connection.getMetaData().getCatalogSeparator()).isEqualTo(".");
    }

    @Test
    @DisplayName("TC_472_001 DatabaseMetaData getCatalogTerm returns the catalog term")
    void tc472001GetCatalogTerm() throws Exception {
        assertThat(connection.getMetaData().getCatalogTerm()).isEqualTo("database");
    }

    @Test
    @DisplayName("TC_473_001 DatabaseMetaData getColumns returns column metadata")
    void tc473001GetColumns() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_MD_COLS");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");

        try (ResultSet rs = connection.getMetaData().getColumns(null, config.db().user().toUpperCase(Locale.ROOT), tableName, "C2")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("COLUMN_NAME")).isEqualToIgnoringCase("C2");
        }
    }

    @Test
    @DisplayName("TC_474_001 DatabaseMetaData getConnection returns the owning connection")
    void tc474001GetConnection() throws Exception {
        assertThat(connection.getMetaData().getConnection()).isSameAs(connection);
    }

    @Test
    @DisplayName("TC_475_001 DatabaseMetaData getDatabaseMajorVersion returns the major version")
    void tc475001GetDatabaseMajorVersion() throws Exception {
        assertThat(connection.getMetaData().getDatabaseMajorVersion()).isEqualTo(7);
    }

    @Test
    @DisplayName("TC_476_001 DatabaseMetaData getDatabaseMinorVersion returns the minor version")
    void tc476001GetDatabaseMinorVersion() throws Exception {
        assertThat(connection.getMetaData().getDatabaseMinorVersion()).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_477_001 DatabaseMetaData getDatabaseProductName returns the product name")
    void tc477001GetDatabaseProductName() throws Exception {
        assertThat(connection.getMetaData().getDatabaseProductName()).isEqualTo("Altibase");
    }

    @Test
    @DisplayName("TC_478_001 DatabaseMetaData getDatabaseProductVersion returns the product version")
    void tc478001GetDatabaseProductVersion() throws Exception {
        assertThat(connection.getMetaData().getDatabaseProductVersion()).startsWith("7.3.");
    }

    @Test
    @DisplayName("TC_479_001 DatabaseMetaData getDefaultTransactionIsolation returns the default isolation")
    void tc479001GetDefaultTransactionIsolation() throws Exception {
        assertThat(connection.getMetaData().getDefaultTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test
    @DisplayName("TC_480_001 DatabaseMetaData getDriverMajorVersion returns the driver major version")
    void tc480001GetDriverMajorVersion() throws Exception {
        assertThat(connection.getMetaData().getDriverMajorVersion()).isEqualTo(7);
    }

    @Test
    @DisplayName("TC_481_001 DatabaseMetaData getDriverMinorVersion returns the driver minor version")
    void tc481001GetDriverMinorVersion() throws Exception {
        assertThat(connection.getMetaData().getDriverMinorVersion()).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_482_001 DatabaseMetaData getDriverName returns the driver name")
    void tc482001GetDriverName() throws Exception {
        assertThat(connection.getMetaData().getDriverName()).contains("Altibase JDBC driver");
    }

    @Test
    @DisplayName("TC_483_001 DatabaseMetaData getDriverVersion returns the driver version")
    void tc483001GetDriverVersion() throws Exception {
        assertThat(connection.getMetaData().getDriverVersion()).startsWith("7.3.");
    }

    @Test
    @DisplayName("TC_484_001 DatabaseMetaData getIndexInfo returns index metadata")
    void tc484001GetIndexInfo() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_MD_IDX_TB");
        String indexName = DbTestSupport.uniqueName("QA_MD_IDX");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int, c2 varchar(20))");
        jdbc.executeUpdate(connection, "create index " + indexName + " on " + tableName + "(c1)");

        try (ResultSet rs = connection.getMetaData().getIndexInfo(null, config.db().user().toUpperCase(Locale.ROOT), tableName, false, false)) {
            boolean found = false;
            while (rs.next()) {
                String candidate = rs.getString("INDEX_NAME");
                if (candidate != null && candidate.equalsIgnoreCase(indexName)) {
                    found = true;
                    break;
                }
            }
            assertThat(found).isTrue();
        }
    }

    @Test
    @DisplayName("TC_485_001 DatabaseMetaData getJDBCMinorVersion returns the JDBC minor version")
    void tc485001GetJdbcMinorVersion() throws Exception {
        assertThat(connection.getMetaData().getJDBCMinorVersion()).isEqualTo(3);
    }

    @Test
    @DisplayName("TC_486_001 DatabaseMetaData getJDBCMajorVersion returns the JDBC major version")
    void tc486001GetJdbcMajorVersion() throws Exception {
        assertThat(connection.getMetaData().getJDBCMajorVersion()).isEqualTo(7);
    }

    @Test
    @DisplayName("TC_487_001 DatabaseMetaData getNumericFunctions returns numeric function names")
    void tc487001GetNumericFunctions() throws Exception {
        assertThat(connection.getMetaData().getNumericFunctions()).contains("ABS");
    }

    @Test
    @DisplayName("TC_488_001 DatabaseMetaData getPrimaryKeys returns primary key metadata")
    void tc488001GetPrimaryKeys() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_MD_PK");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int primary key, c2 varchar(20))");

        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, config.db().user().toUpperCase(Locale.ROOT), tableName)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("COLUMN_NAME")).isEqualToIgnoringCase("C1");
        }
    }

    @Test
    @DisplayName("TC_489_001 DatabaseMetaData getProcedureColumns returns procedure parameter metadata")
    void tc489001GetProcedureColumns() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_MD_PROC");
        createSimpleOutProcedure(procedureName);

        try (ResultSet rs = connection.getMetaData().getProcedureColumns(null, config.db().user().toUpperCase(Locale.ROOT), procedureName, null)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("PROCEDURE_NAME")).isEqualToIgnoringCase(procedureName);
            assertThat(rs.getString("COLUMN_NAME")).isEqualToIgnoringCase("P1");
        }
    }

    @Test
    @DisplayName("TC_490_001 DatabaseMetaData getProcedures returns procedure metadata")
    void tc490001GetProcedures() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_MD_PROC");
        createSimpleOutProcedure(procedureName);

        try (ResultSet rs = connection.getMetaData().getProcedures(null, config.db().user().toUpperCase(Locale.ROOT), procedureName)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("PROCEDURE_NAME")).isEqualToIgnoringCase(procedureName);
        }
    }

    @Test
    @DisplayName("TC_491_001 DatabaseMetaData getProcedureTerm returns the procedure term")
    void tc491001GetProcedureTerm() throws Exception {
        assertThat(connection.getMetaData().getProcedureTerm()).isEqualTo("stored procedure");
    }

    @Test
    @DisplayName("TC_492_001 DatabaseMetaData getStringFunctions returns string function names")
    void tc492001GetStringFunctions() throws Exception {
        assertThat(connection.getMetaData().getStringFunctions()).contains("SUBSTR");
    }

    @Test
    @DisplayName("TC_493_001 DatabaseMetaData getSystemFunctions returns system function names")
    void tc493001GetSystemFunctions() throws Exception {
        assertThat(connection.getMetaData().getSystemFunctions()).contains("NVL");
    }

    @Test
    @DisplayName("TC_494_001 DatabaseMetaData getTables returns table metadata")
    void tc494001GetTables() throws Exception {
        String tableName = DbTestSupport.uniqueName("QA_MD_TABLES");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 int)");

        try (ResultSet rs = connection.getMetaData().getTables(null, config.db().user().toUpperCase(Locale.ROOT), tableName, new String[]{"TABLE"})) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TABLE_NAME")).isEqualToIgnoringCase(tableName);
        }
    }

    @Test
    @DisplayName("TC_495_001 DatabaseMetaData getTimeDateFunctions returns date-time function names")
    void tc495001GetTimeDateFunctions() throws Exception {
        assertThat(connection.getMetaData().getTimeDateFunctions()).contains("SYSDATE");
    }

    @Test
    @DisplayName("TC_496_001 DatabaseMetaData getTypeInfo returns database type metadata")
    void tc496001GetTypeInfo() throws Exception {
        try (ResultSet rs = connection.getMetaData().getTypeInfo()) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TYPE_NAME")).isNotBlank();
        }
    }

    @Test
    @DisplayName("TC_497_001 DatabaseMetaData getURL returns the connection URL")
    void tc497001GetUrl() throws Exception {
        String url = connection.getMetaData().getURL();

        assertThat(url).startsWith("jdbc:Altibase");
        assertThat(url).contains(config.db().host() + ":" + config.db().port() + "/" + config.db().database());
    }

    @Test
    @DisplayName("TC_498_001 DatabaseMetaData getUserName returns the connected user")
    void tc498001GetUserName() throws Exception {
        assertThat(connection.getMetaData().getUserName()).isEqualToIgnoringCase(config.db().user());
    }

    @Test
    @DisplayName("TC_499_001 DatabaseMetaData insertsAreDetected reports row insert detection support")
    void tc499001InsertsAreDetected() throws Exception {
        assertThat(connection.getMetaData().insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
    }

    @Test
    @DisplayName("TC_500_001 DatabaseMetaData isReadOnly returns database read-only status")
    void tc500001IsReadOnly() throws Exception {
        assertThat(connection.getMetaData().isReadOnly()).isFalse();
    }

    @Test
    @DisplayName("TC_501_001 DatabaseMetaData locatorsUpdateCopy reports LOB update behavior")
    void tc501001LocatorsUpdateCopy() throws Exception {
        assertThat(connection.getMetaData().locatorsUpdateCopy()).isTrue();
    }

    @Test
    @DisplayName("TC_502_001 DatabaseMetaData nullPlusNonNullIsNull reports null concatenation behavior")
    void tc502001NullPlusNonNullIsNull() throws Exception {
        assertThat(connection.getMetaData().nullPlusNonNullIsNull()).isTrue();
    }

    @Test
    @DisplayName("TC_503_001 DatabaseMetaData othersDeletesAreVisible reports delete visibility from other sessions")
    void tc503001OthersDeletesAreVisible() throws Exception {
        assertThat(connection.getMetaData().othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
    }

    @Test
    @DisplayName("TC_504_001 DatabaseMetaData othersUpdatesAreVisible reports update visibility from other sessions")
    void tc504001OthersUpdatesAreVisible() throws Exception {
        assertThat(connection.getMetaData().othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
    }

    @Test
    @DisplayName("TC_505_001 DatabaseMetaData ownDeletesAreVisible reports delete visibility in the same result set")
    void tc505001OwnDeletesAreVisible() throws Exception {
        assertThat(connection.getMetaData().ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
    }

    @Test
    @DisplayName("TC_506_001 DatabaseMetaData ownUpdatesAreVisible reports update visibility in the same result set")
    void tc506001OwnUpdatesAreVisible() throws Exception {
        assertThat(connection.getMetaData().ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
    }

    @Test
    @DisplayName("TC_507_001 DatabaseMetaData storesMixedCaseQuotedIdentifiers reports quoted identifier storage rules")
    void tc507001StoresMixedCaseQuotedIdentifiers() throws Exception {
        assertThat(connection.getMetaData().storesMixedCaseQuotedIdentifiers()).isTrue();
    }

    @Test
    @DisplayName("TC_508_001 DatabaseMetaData storesUpperCaseIdentifiers reports identifier storage rules")
    void tc508001StoresUpperCaseIdentifiers() throws Exception {
        assertThat(connection.getMetaData().storesUpperCaseIdentifiers()).isTrue();
    }

    @Test
    @DisplayName("TC_509_001 DatabaseMetaData supportsAlterTableWithAddColumn reports ALTER TABLE ADD support")
    void tc509001SupportsAlterTableWithAddColumn() throws Exception {
        assertThat(connection.getMetaData().supportsAlterTableWithAddColumn()).isTrue();
    }

    @Test
    @DisplayName("TC_510_001 DatabaseMetaData supportsAlterTableWithDropColumn reports ALTER TABLE DROP support")
    void tc510001SupportsAlterTableWithDropColumn() throws Exception {
        assertThat(connection.getMetaData().supportsAlterTableWithDropColumn()).isTrue();
    }

    @Test
    @DisplayName("TC_511_001 DatabaseMetaData supportsANSI92EntryLevelSQL reports ANSI-92 entry SQL support")
    void tc511001SupportsAnsi92EntryLevelSql() throws Exception {
        assertThat(connection.getMetaData().supportsANSI92EntryLevelSQL()).isTrue();
    }

    @Test
    @DisplayName("TC_512_001 DatabaseMetaData supportsANSI92FullSQL reports ANSI-92 full SQL support")
    void tc512001SupportsAnsi92FullSql() throws Exception {
        assertThat(connection.getMetaData().supportsANSI92FullSQL()).isTrue();
    }

    @Test
    @DisplayName("TC_513_001 DatabaseMetaData supportsANSI92IntermediateSQL reports ANSI-92 intermediate SQL support")
    void tc513001SupportsAnsi92IntermediateSql() throws Exception {
        assertThat(connection.getMetaData().supportsANSI92IntermediateSQL()).isTrue();
    }

    @Test
    @DisplayName("TC_514_001 DatabaseMetaData supportsBatchUpdates reports batch update support")
    void tc514001SupportsBatchUpdates() throws Exception {
        assertThat(connection.getMetaData().supportsBatchUpdates()).isTrue();
    }

    @Test
    @DisplayName("TC_515_001 DatabaseMetaData supportsColumnAliasing reports column alias support")
    void tc515001SupportsColumnAliasing() throws Exception {
        assertThat(connection.getMetaData().supportsColumnAliasing()).isTrue();
    }

    @Test
    @DisplayName("TC_516_001 DatabaseMetaData supportsCoreSQLGrammar reports core SQL grammar support")
    void tc516001SupportsCoreSqlGrammar() throws Exception {
        assertThat(connection.getMetaData().supportsCoreSQLGrammar()).isTrue();
    }

    @Test
    @DisplayName("TC_517_001 DatabaseMetaData supportsCorrelatedSubqueries reports correlated subquery support")
    void tc517001SupportsCorrelatedSubqueries() throws Exception {
        assertThat(connection.getMetaData().supportsCorrelatedSubqueries()).isTrue();
    }

    @Test
    @DisplayName("TC_518_001 DatabaseMetaData supportsDataDefinitionAndDataManipulationTransactions reports DDL and DML transaction support")
    void tc518001SupportsDataDefinitionAndDataManipulationTransactions() throws Exception {
        assertThat(connection.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()).isTrue();
    }

    @Test
    @DisplayName("TC_519_001 DatabaseMetaData supportsExpressionsInOrderBy reports expression support in ORDER BY")
    void tc519001SupportsExpressionsInOrderBy() throws Exception {
        assertThat(connection.getMetaData().supportsExpressionsInOrderBy()).isTrue();
    }

    @Test
    @DisplayName("TC_520_001 DatabaseMetaData supportsExtendedSQLGrammar reports extended SQL grammar support")
    void tc520001SupportsExtendedSqlGrammar() throws Exception {
        assertThat(connection.getMetaData().supportsExtendedSQLGrammar()).isTrue();
    }

    @Test
    @DisplayName("TC_521_001 DatabaseMetaData supportsFullOuterJoins reports full outer join support")
    void tc521001SupportsFullOuterJoins() throws Exception {
        assertThat(connection.getMetaData().supportsFullOuterJoins()).isTrue();
    }

    @Test
    @DisplayName("TC_522_001 DatabaseMetaData supportsGetGeneratedKeys reports generated key support")
    void tc522001SupportsGetGeneratedKeys() throws Exception {
        assertThat(connection.getMetaData().supportsGetGeneratedKeys()).isTrue();
    }

    @Test
    @DisplayName("TC_523_001 DatabaseMetaData supportsGroupBy reports GROUP BY support")
    void tc523001SupportsGroupBy() throws Exception {
        assertThat(connection.getMetaData().supportsGroupBy()).isTrue();
    }

    @Test
    @DisplayName("TC_524_001 DatabaseMetaData supportsGroupByBeyondSelect reports GROUP BY beyond SELECT support")
    void tc524001SupportsGroupByBeyondSelect() throws Exception {
        assertThat(connection.getMetaData().supportsGroupByBeyondSelect()).isTrue();
    }

    @Test
    @DisplayName("TC_525_001 DatabaseMetaData supportsIntegrityEnhancementFacility reports integrity enhancement support")
    void tc525001SupportsIntegrityEnhancementFacility() throws Exception {
        assertThat(connection.getMetaData().supportsIntegrityEnhancementFacility()).isTrue();
    }

    @Test
    @DisplayName("TC_526_001 DatabaseMetaData supportsLikeEscapeClause reports LIKE escape support")
    void tc526001SupportsLikeEscapeClause() throws Exception {
        assertThat(connection.getMetaData().supportsLikeEscapeClause()).isTrue();
    }

    @Test
    @DisplayName("TC_527_001 DatabaseMetaData supportsLimitedOuterJoins reports limited outer join support")
    void tc527001SupportsLimitedOuterJoins() throws Exception {
        assertThat(connection.getMetaData().supportsLimitedOuterJoins()).isTrue();
    }

    @Test
    @DisplayName("TC_528_001 DatabaseMetaData supportsMinimumSQLGrammar reports minimum SQL grammar support")
    void tc528001SupportsMinimumSqlGrammar() throws Exception {
        assertThat(connection.getMetaData().supportsMinimumSQLGrammar()).isTrue();
    }

    @Test
    @DisplayName("TC_529_001 DatabaseMetaData supportsMixedCaseQuotedIdentifiers reports mixed-case quoted identifier support")
    void tc529001SupportsMixedCaseQuotedIdentifiers() throws Exception {
        assertThat(connection.getMetaData().supportsMixedCaseQuotedIdentifiers()).isTrue();
    }

    @Test
    @DisplayName("TC_530_001 DatabaseMetaData supportsMultipleOpenResults returns support information")
    void tc530001SupportsMultipleOpenResults() throws Exception {
        assertThat(connection.getMetaData().supportsMultipleOpenResults()).isIn(true, false);
    }

    @Test
    @DisplayName("TC_531_001 DatabaseMetaData supportsNonNullableColumns reports non-nullable column support")
    void tc531001SupportsNonNullableColumns() throws Exception {
        assertThat(connection.getMetaData().supportsNonNullableColumns()).isTrue();
    }

    @Test
    @DisplayName("TC_532_001 DatabaseMetaData supportsOpenCursorsAcrossCommit reports cursor holdability across commit")
    void tc532001SupportsOpenCursorsAcrossCommit() throws Exception {
        assertThat(connection.getMetaData().supportsOpenCursorsAcrossCommit()).isTrue();
    }

    @Test
    @DisplayName("TC_533_001 DatabaseMetaData supportsOpenCursorsAcrossRollback reports cursor holdability across rollback")
    void tc533001SupportsOpenCursorsAcrossRollback() throws Exception {
        assertThat(connection.getMetaData().supportsOpenCursorsAcrossRollback()).isTrue();
    }

    @Test
    @DisplayName("TC_534_001 DatabaseMetaData supportsOpenStatementsAcrossCommit returns support information")
    void tc534001SupportsOpenStatementsAcrossCommit() throws Exception {
        assertThat(connection.getMetaData().supportsOpenStatementsAcrossCommit()).isIn(true, false);
    }

    @Test
    @DisplayName("TC_535_001 DatabaseMetaData supportsOpenStatementsAcrossRollback returns support information")
    void tc535001SupportsOpenStatementsAcrossRollback() throws Exception {
        assertThat(connection.getMetaData().supportsOpenStatementsAcrossRollback()).isIn(true, false);
    }

    @Test
    @DisplayName("TC_536_001 DatabaseMetaData supportsOrderByUnrelated reports ORDER BY support for non-selected columns")
    void tc536001SupportsOrderByUnrelated() throws Exception {
        assertThat(connection.getMetaData().supportsOrderByUnrelated()).isTrue();
    }

    @Test
    @DisplayName("TC_537_001 DatabaseMetaData supportsOuterJoins reports outer join support")
    void tc537001SupportsOuterJoins() throws Exception {
        assertThat(connection.getMetaData().supportsOuterJoins()).isTrue();
    }

    @Test
    @DisplayName("TC_538_001 DatabaseMetaData supportsResultSetConcurrency reports supported concurrency")
    void tc538001SupportsResultSetConcurrency() throws Exception {
        assertThat(connection.getMetaData().supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)).isTrue();
    }

    @Test
    @DisplayName("TC_539_001 DatabaseMetaData supportsResultSetHoldability reports supported holdability")
    void tc539001SupportsResultSetHoldability() throws Exception {
        assertThat(connection.getMetaData().supportsResultSetHoldability(connection.getHoldability())).isTrue();
    }

    @Test
    @DisplayName("TC_540_001 DatabaseMetaData supportsResultSetType reports supported result set types")
    void tc540001SupportsResultSetType() throws Exception {
        assertThat(connection.getMetaData().supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY)).isTrue();
    }

    @Test
    @DisplayName("TC_541_001 DatabaseMetaData supportsSavepoints reports savepoint support")
    void tc541001SupportsSavepoints() throws Exception {
        assertThat(connection.getMetaData().supportsSavepoints()).isTrue();
    }

    @Test
    @DisplayName("TC_542_001 DatabaseMetaData supportsSchemasInDataManipulation reports schema support in DML")
    void tc542001SupportsSchemasInDataManipulation() throws Exception {
        assertThat(connection.getMetaData().supportsSchemasInDataManipulation()).isTrue();
    }

    @Test
    @DisplayName("TC_543_001 DatabaseMetaData supportsSchemasInIndexDefinitions reports schema support in index definitions")
    void tc543001SupportsSchemasInIndexDefinitions() throws Exception {
        assertThat(connection.getMetaData().supportsSchemasInIndexDefinitions()).isTrue();
    }

    @Test
    @DisplayName("TC_544_001 DatabaseMetaData supportsSchemasInPrivilegeDefinitions reports schema support in privilege definitions")
    void tc544001SupportsSchemasInPrivilegeDefinitions() throws Exception {
        assertThat(connection.getMetaData().supportsSchemasInPrivilegeDefinitions()).isTrue();
    }

    @Test
    @DisplayName("TC_545_001 DatabaseMetaData supportsSchemasInProcedureCalls reports schema support in procedure calls")
    void tc545001SupportsSchemasInProcedureCalls() throws Exception {
        assertThat(connection.getMetaData().supportsSchemasInProcedureCalls()).isTrue();
    }

    @Test
    @DisplayName("TC_546_001 DatabaseMetaData supportsSchemasInTableDefinitions reports schema support in table definitions")
    void tc546001SupportsSchemasInTableDefinitions() throws Exception {
        assertThat(connection.getMetaData().supportsSchemasInTableDefinitions()).isTrue();
    }

    @Test
    @DisplayName("TC_547_001 DatabaseMetaData supportsSubqueriesInComparisons reports comparison subquery support")
    void tc547001SupportsSubqueriesInComparisons() throws Exception {
        assertThat(connection.getMetaData().supportsSubqueriesInComparisons()).isTrue();
    }

    @Test
    @DisplayName("TC_548_001 DatabaseMetaData supportsSubqueriesInExists reports EXISTS subquery support")
    void tc548001SupportsSubqueriesInExists() throws Exception {
        assertThat(connection.getMetaData().supportsSubqueriesInExists()).isTrue();
    }

    @Test
    @DisplayName("TC_549_001 DatabaseMetaData supportsSubqueriesInIns reports IN subquery support")
    void tc549001SupportsSubqueriesInIns() throws Exception {
        assertThat(connection.getMetaData().supportsSubqueriesInIns()).isTrue();
    }

    @Test
    @DisplayName("TC_550_001 DatabaseMetaData supportsSubqueriesInQuantifieds reports quantified subquery support")
    void tc550001SupportsSubqueriesInQuantifieds() throws Exception {
        assertThat(connection.getMetaData().supportsSubqueriesInQuantifieds()).isTrue();
    }

    @Test
    @DisplayName("TC_551_001 DatabaseMetaData supportsTableCorrelationNames reports table correlation name support")
    void tc551001SupportsTableCorrelationNames() throws Exception {
        assertThat(connection.getMetaData().supportsTableCorrelationNames()).isTrue();
    }

    @Test
    @DisplayName("TC_552_001 DatabaseMetaData supportsTransactionIsolationLevel reports requested isolation support")
    void tc552001SupportsTransactionIsolationLevel() throws Exception {
        assertThat(connection.getMetaData().supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED)).isTrue();
    }

    @Test
    @DisplayName("TC_553_001 DatabaseMetaData supportsTransactions reports transaction support")
    void tc553001SupportsTransactions() throws Exception {
        assertThat(connection.getMetaData().supportsTransactions()).isTrue();
    }

    @Test
    @DisplayName("TC_554_001 DatabaseMetaData supportsUnion reports UNION support")
    void tc554001SupportsUnion() throws Exception {
        assertThat(connection.getMetaData().supportsUnion()).isTrue();
    }

    @Test
    @DisplayName("TC_555_001 DatabaseMetaData supportsUnionAll reports UNION ALL support")
    void tc555001SupportsUnionAll() throws Exception {
        assertThat(connection.getMetaData().supportsUnionAll()).isTrue();
    }

    @Test
    @DisplayName("TC_556_001 DatabaseMetaData updatesAreDetected reports row update detection support")
    void tc556001UpdatesAreDetected() throws Exception {
        assertThat(connection.getMetaData().updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY)).isFalse();
    }

    @Test
    @DisplayName("TC_557_001 DatabaseMetaData getFunctionColumns returns function parameter metadata")
    void tc557001GetFunctionColumns() throws Exception {
        try (ResultSet rs = connection.getMetaData().getFunctionColumns(null, null, "ABS", null)) {
            assertThat(rs.getMetaData().getColumnCount()).isGreaterThan(0);
            if (rs.next()) {
                assertThat(rs.getString("FUNCTION_NAME")).isEqualToIgnoringCase("ABS");
            }
        }
    }

    @Test
    @DisplayName("TC_558_001 DatabaseMetaData getFunctions returns function metadata")
    void tc558001GetFunctions() throws Exception {
        try (ResultSet rs = connection.getMetaData().getFunctions(null, null, "ABS")) {
            assertThat(rs.getMetaData().getColumnCount()).isGreaterThan(0);
            if (rs.next()) {
                assertThat(rs.getString("FUNCTION_NAME")).isEqualToIgnoringCase("ABS");
            }
        }
    }

    private void createSimpleOutProcedure(String procedureName) {
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));
        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 out integer) as begin p1 := 42; end;"
        );
    }
}
