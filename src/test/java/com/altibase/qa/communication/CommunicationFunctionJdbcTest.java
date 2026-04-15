package com.altibase.qa.communication;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Types;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class CommunicationFunctionJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("TC_227_001 MSG_CREATE_QUEUE creates a message queue for the given key")
    void tc227001MsgCreateQueue() {
        int queueKey = uniqueQueueKey();
        registerCleanup(() -> dropQueueQuietly(queueKey));

        assertThat(jdbc.queryForString(connection, "select msg_create_queue(" + queueKey + ") from dual")).isEqualTo("0");
    }

    @Test
    @DisplayName("TC_228_001 MSG_DROP_QUEUE removes a message queue for the given key")
    void tc228001MsgDropQueue() {
        int queueKey = uniqueQueueKey();

        assertThat(jdbc.queryForString(connection, "select msg_create_queue(" + queueKey + ") from dual")).isEqualTo("0");
        assertThat(jdbc.queryForString(connection, "select msg_drop_queue(" + queueKey + ") from dual")).isEqualTo("0");
    }

    @Test
    @DisplayName("TC_229_001 MSG_SND_QUEUE sends a message to a queue")
    void tc229001MsgSendQueue() {
        int queueKey = uniqueQueueKey();
        registerCleanup(() -> dropQueueQuietly(queueKey));

        assertThat(jdbc.queryForString(connection, "select msg_create_queue(" + queueKey + ") from dual")).isEqualTo("0");
        assertThat(jdbc.queryForString(connection,
                "select msg_snd_queue(" + queueKey + ", varchar'altibase') from dual")).isEqualTo("0");
    }

    @Test
    @DisplayName("TC_230_001 MSG_RCV_QUEUE receives a queued message")
    void tc230001MsgReceiveQueue() {
        int queueKey = uniqueQueueKey();
        registerCleanup(() -> dropQueueQuietly(queueKey));

        assertThat(jdbc.queryForString(connection, "select msg_create_queue(" + queueKey + ") from dual")).isEqualTo("0");
        assertThat(jdbc.queryForString(connection,
                "select msg_snd_queue(" + queueKey + ", varchar'altibase') from dual")).isEqualTo("0");

        assertThat(jdbc.queryForString(connection,
                "select raw_to_varchar(msg_rcv_queue(" + queueKey + ")) from dual")).isEqualTo("altibase");
    }

    @Test
    @DisplayName("TC_354_001 CLOSEALL_CONNECT closes all connection handles in a stored procedure")
    void tc354001CloseAllConnect() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CLOSEALL_CONN");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 out integer) as " +
                        "v1 connect_type; v2 connect_type; begin " +
                        "v1 := open_connect('127.0.0.1', " + config.db().port() + ", 1000, 3000); " +
                        "v2 := open_connect('127.0.0.1', " + config.db().port() + ", 1000, 3000); " +
                        "p1 := closeall_connect(); end;"
        );

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();

            assertThat(cs.getInt(1)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("TC_355_001 CLOSE_CONNECT closes an individual connection handle")
    void tc355001CloseConnect() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_CLOSE_CONN");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 out integer) as " +
                        "v1 connect_type; v2 integer; begin " +
                        "v1 := open_connect('127.0.0.1', " + config.db().port() + ", 1000, 3000); " +
                        "v2 := write_raw(v1, to_raw('MESSAGE'), raw_sizeof('MESSAGE')); " +
                        "p1 := close_connect(v1); end;"
        );

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();

            assertThat(cs.getInt(1)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("TC_356_001 IS_CONNECTED reports connection state for CONNECT_TYPE handles")
    void tc356001IsConnected() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_IS_CONN");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 out integer, p2 out integer) as " +
                        "v1 connect_type; begin " +
                        "v1 := open_connect('127.0.0.1', " + config.db().port() + ", 1000, 3000); " +
                        "p1 := is_connected(v1); " +
                        "p2 := close_connect(v1); end;"
        );

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?,?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();

            assertThat(cs.getInt(1)).isEqualTo(0);
            assertThat(cs.getInt(2)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("TC_357_001 OPEN_CONNECT opens a TCP connection handle to the target host and port")
    void tc357001OpenConnect() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_OPEN_CONN");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 out integer, p2 out integer) as " +
                        "v1 connect_type; begin " +
                        "v1 := open_connect('127.0.0.1', " + config.db().port() + ", 1000, 3000); " +
                        "p1 := is_connected(v1); " +
                        "p2 := close_connect(v1); end;"
        );

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?,?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();

            assertThat(cs.getInt(1)).isEqualTo(0);
            assertThat(cs.getInt(2)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("TC_358_001 WRITE_RAW writes RAW payloads to an open connection handle")
    void tc358001WriteRaw() throws Exception {
        String procedureName = DbTestSupport.uniqueName("QA_WRITE_RAW");
        registerCleanup(() -> DbTestSupport.dropProcedureQuietly(jdbc, connection, procedureName));

        jdbc.executeUpdate(
                connection,
                "create or replace procedure " + procedureName + "(p1 out integer, p2 out integer) as " +
                        "v1 connect_type; begin " +
                        "v1 := open_connect('127.0.0.1', " + config.db().port() + ", 1000, 3000); " +
                        "p1 := write_raw(v1, to_raw('MESSAGE'), raw_sizeof('MESSAGE')); " +
                        "p2 := close_connect(v1); end;"
        );

        try (CallableStatement cs = connection.prepareCall("{call " + procedureName + "(?,?)}")) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();

            assertThat(cs.getInt(1)).isGreaterThan(0);
            assertThat(cs.getInt(2)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("TC_239_001 SENDMSG sends a datagram and returns the message length")
    void tc239001SendMsg() {
        String message = "THIS IS A MESSAGE";

        int sentLength = Integer.parseInt(
                jdbc.queryForString(connection, "select sendmsg('127.0.0.1', 12345, '" + message + "', 1) from dual")
        );

        assertThat(sentLength).isEqualTo(message.length());
    }

    @Test
    @DisplayName("Additional negative case: MSG_CREATE_QUEUE returns 1 for duplicate keys")
    void msgCreateQueueDuplicateKeyReturnsOne() {
        int queueKey = uniqueQueueKey();
        registerCleanup(() -> dropQueueQuietly(queueKey));

        assertThat(jdbc.queryForString(connection, "select msg_create_queue(" + queueKey + ") from dual")).isEqualTo("0");
        assertThat(jdbc.queryForString(connection, "select msg_create_queue(" + queueKey + ") from dual")).isEqualTo("1");
    }

    @Test
    @DisplayName("Additional negative case: MSG_DROP_QUEUE returns 1 when the queue does not exist")
    void msgDropQueueMissingKeyReturnsOne() {
        int queueKey = uniqueQueueKey();

        assertThat(jdbc.queryForString(connection, "select msg_drop_queue(" + queueKey + ") from dual")).isEqualTo("1");
    }

    private int uniqueQueueKey() {
        return ThreadLocalRandom.current().nextInt(100000, 999999);
    }

    private void dropQueueQuietly(int queueKey) {
        try {
            jdbc.queryForString(connection, "select msg_drop_queue(" + queueKey + ") from dual");
        } catch (Exception ignored) {
        }
    }
}
