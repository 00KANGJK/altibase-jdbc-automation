package com.altibase.qa.communication;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import com.altibase.qa.support.FeatureProbe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("SqlNoDataSourceInspection")
class NetworkPackageJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional environment case: TCP connection control writes to configured echo endpoint")
    void tcpConnectionControlWritesToConfiguredEchoEndpoint() throws Exception {
        FeatureProbe.assumeUtlTcpAvailable(config, jdbc, connection);

        String procedureName = DbTestSupport.uniqueName("QA_TCP_ECHO");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p_connected out integer, p_written out integer, p_closed out integer) as " +
                        "c connect_type; " +
                        "begin " +
                        "c := open_connect('" + config.network().tcpHost() + "', " + config.network().tcpPort() + ", 1000, 3000); " +
                        "p_connected := is_connected(c); " +
                        "p_written := write_raw(c, to_raw('PING' || chr(10)), raw_sizeof('PING' || chr(10))); " +
                        "p_closed := close_connect(c); " +
                        "end;"
        );

        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(?,?,?)}")) {
            callableStatement.registerOutParameter(1, Types.INTEGER);
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.execute();

            assertThat(callableStatement.getInt(1)).isEqualTo(0);
            assertThat(callableStatement.getInt(2)).isGreaterThan(0);
            assertThat(callableStatement.getInt(3)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("Additional environment case: UTL_SMTP sends a message to configured debug SMTP endpoint")
    void utlSmtpSendsMessageToConfiguredDebugEndpoint() throws Exception {
        FeatureProbe.assumeUtlSmtpAvailable(config, jdbc, connection);

        String procedureName = DbTestSupport.uniqueName("QA_UTL_SMTP");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p_response out varchar(65534)) as " +
                        "c connect_type; " +
                        "r varchar(65534); " +
                        "begin " +
                        "c := utl_smtp.open_connection('" + config.network().smtpHost() + "', " + config.network().smtpPort() + ", null); " +
                        "r := utl_smtp.helo(c, 'localhost'); " +
                        "r := utl_smtp.mail(c, 'qa@example.com'); " +
                        "r := utl_smtp.rcpt(c, 'qa@example.com'); " +
                        "r := utl_smtp.open_data(c); " +
                        "utl_smtp.write_data(c, 'Subject: altibase qa' || chr(13) || chr(10) || chr(13) || chr(10) || 'hello'); " +
                        "r := utl_smtp.close_data(c); " +
                        "p_response := utl_smtp.quit(c); " +
                        "end;"
        );

        try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(?)}")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.execute();

            assertThat(callableStatement.getString(1)).contains("221");
        }
    }
}
