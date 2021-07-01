package mariadbcdc.binlog;

public enum MysqlCapabilityFlag {
    /**
     * Use the improved version of Old Password Authentication.
     */
    CLIENT_LONG_PASSWORD(0x00000001),
    /**
     * Send found rows instead of affected rows in EOF_Packet.
     */
    CLIENT_FOUND_ROWS(0x00000002),
    /**
     * Longer flags in Protocol::ColumnDefinition320.<br/>
     * Server: Supports longer flags., Client: Expects longer flags.
     */
    CLIENT_LONG_FLAG( 0x00000004),
    /**
     * Database (schema) name can be specified on connect in Handshake Response Packet.<br/>
     * Server: Supports schema-name in Handshake Response Packet.<br/>
     * Client: Handshake Response Packet contains a schema-name.
     */
    CLIENT_CONNECT_WITH_DB(0x00000008),
    /**
     * Server: Do not permit database.table.column.
     */
    CLIENT_NO_SCHEMA(0x00000010),
    /**
     * Compression protocol supported.<br/>
     * Server: Supports compression.<br/>
     * Client: Switches to Compression compressed protocol after successful authentication.
     */
    CLIENT_COMPRESS(0x00000020),
    /**
     * Special handling of ODBC behavior.<br/>
     * Note : No special behavior since 3.22.
     */
    CLIENT_ODBC(0x00000040),
    /**
     * Can use LOAD DATA LOCAL.<br/>
     * Server: Enables the LOCAL INFILE request of LOAD DATA|XML.<br/>
     * Client: Will handle LOCAL INFILE request.
     */
    CLIENT_LOCAL_FILES(0x00000080),
    /**
     * Server: Parser can ignore spaces before '('.<br/>
     * Client: Let the parser ignore spaces before '('.
     */
    CLIENT_IGNORE_SPACE(0x00000100),
    /**
     * Server: Supports the 4.1 protocol.<br/>
     * Client: Uses the 4.1 protocol.<br/>
     * Note: this value was CLIENT_CHANGE_USER in 3.22, unused in 4.0
     */
    CLIENT_PROTOCOL_41(0x00000200),
    /**
     * wait_timeout versus wait_interactive_timeout.<br/>
     * Server: Supports interactive and noninteractive clients.<br/>
     * Client: Client is interactive.<br/>
     * See: mysql_real_connect()
     */
    CLIENT_INTERACTIVE(0x00000400),
    /**
     * Server: Supports SSL<br/>
     * Client: Switch to SSL after sending the capability-flags.
     */
    CLIENT_SSL(0x00000800),
    /**
     * Client: Do not issue SIGPIPE if network failures occur (libmysqlclient only).<br/>
     * See: mysql_real_connect()
     */
    CLIENT_IGNORE_SIGPIPE(0x00001000),
    /**
     * Server: Can send status flags in EOF_Packet.<br/>
     * Client: Expects status flags in EOF_Packet.<br/>
     * Note: This flag is optional in 3.23, but always set by the server since 4.0.
     */
    CLIENT_TRANSACTIONS(0x00002000),
    /**
     * Unused.<br/>
     * Note: Was named CLIENT_PROTOCOL_41 in 4.1.0.
     * @see MysqlCapabilityFlag.CLIENT_PROTOCOL_41
     */
    CLIENT_RESERVED(0x00004000),
    /**
     * Server: Supports Authentication::Native41.<br/>
     * Client: Supports Authentication::Native41.
     */
    CLIENT_SECURE_CONNECTION(0x00008000),
    /**
     * Server: Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.<br/>
     * Client: May send multiple statements per COM_QUERY and COM_STMT_PREPARE.<br/>
     * Note: Was named CLIENT_MULTI_QUERIES in 4.1.0, renamed later.<br/>
     * Requires: CLIENT_PROTOCOL_41
     */
    CLIENT_MULTI_STATEMENTS(0x00010000),
    /**
     * Server: Can send multiple resultsets for COM_QUERY.<br/>
     * Client: Can handle multiple resultsets for COM_QUERY.<br/>
     * Requires: CLIENT_PROTOCOL_41
     */
    CLIENT_MULTI_RESULTS(0x00020000),
    /**
     * Server: Can send multiple resultsets for COM_STMT_EXECUTE.<br/>
     * Client: Can handle multiple resultsets for COM_STMT_EXECUTE.<br/>
     * Requires: CLIENT_PROTOCOL_41
     */
    CLIENT_PS_MULTI_RESULTS(0x00040000),
    /**
     * Server: Sends extra data in Initial Handshake Packet and supports the pluggable authentication protocol.<br/>
     * Client: Supports authentication plugins.<br/>
     * Requires: CLIENT_PROTOCOL_41
     */
    CLIENT_PLUGIN_AUTH(0x00080000),
    /**
     * Server: Permits connection attributes in Protocol::HandshakeResponse41.<br/>
     * Client: Sends connection attributes in Protocol::HandshakeResponse41.
     */
    CLIENT_CONNECT_ATTRS(0x00100000),
    /**
     * Server: Understands length-encoded integer for auth response data in Protocol::HandshakeResponse41.<br/>
     * Client: Length of auth response data in Protocol::HandshakeResponse41 is a length-encoded integer.<br/>
     * Note: The flag was introduced in 5.6.6, but had the wrong value.
     */
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA(0x00200000),
    /**
     * Server: Announces support for expired password extension.<br/>
     * Client: Can handle expired passwords.
     */
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS(0x00400000),
    /**
     * Server: Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send session-state change data after a OK packet.<br/>
     * Client: Expects the server to send sesson-state changes after a OK packet.
     */
    CLIENT_SESSION_TRACK(0x00800000),
    /**
     * Server: Can send OK after a Text Resultset.<br/>
     * Client: Expects an OK (instead of EOF) after the resultset rows of a Text Resultset.<br/>
     * Background: To support CLIENT_SESSION_TRACK, additional information must be sent after all successful commands. Although the OK packet is extensible, the EOF packet is not due to the overlap of its bytes with the content of the Text Resultset Row.<br/>
     * Therefore, the EOF packet in the Text Resultset is replaced with an OK packet. EOF packets are deprecated as of MySQL 5.7.5.
     */
    CLIENT_DEPRECATE_EOF(0x01000000),
    ;

    private int value;

    MysqlCapabilityFlag(int value) {
        this.value = value;
    }

    public boolean support(int capabilities) {
        return (capabilities & this.value) != 0;
    }

    public int getValue() {
        return value;
    }
}
