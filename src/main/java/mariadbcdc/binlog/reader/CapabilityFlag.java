package mariadbcdc.binlog.reader;

public enum CapabilityFlag {
    CLIENT_MYSQL(1),
    FOUND_ROWS(2),
    /**
     * One can specify db on connect
     */
    CONNECT_WITH_DB(8),
    /**
     * Can use compression protocol
     */
    COMPRESS(32),
    /**
     * Can use LOAD DATA LOCAL
     */
    LOCAL_FILES(128),
    /**
     * Ignore spaces before '('
     */
    IGNORE_SPACE(256),
    /**
     * 4.1 protocol
     */
    CLIENT_PROTOCOL_41(1 << 9),
    CLIENT_INTERACTIVE(1 << 10),
    /**
     * Can use SSL
     */
    SSL(1 << 11),
    TRANSACTIONS(1 << 12),
    /**
     * 4.1 authentication
     */
    SECURE_CONNECTION(1 << 13),
    /**
     * Enable/disable multi-stmt support
     */
    MULTI_STATEMENTS(1 << 16),
    /**
     * Enable/disable multi-results
     */
    MULTI_RESULTS(1 << 17),
    /**
     * Enable/disable multi-results for PrepareStatement
     */
    PS_MULTI_RESULTS(1 << 18),
    /**
     * Client supports plugin authentication
     */
    PLUGIN_AUTH(1 << 19),
    /**
     * Client send connection attributes
     */
    CONNECT_ATTRS(1 << 20),
    /**
     * Enable authentication response packet to be larger than 255 bytes
     */
    PLUGIN_AUTH_LENENC_CLIENT_DATA(1 << 21),
    /**
     * Enable/disable session tracking in OK_Packet
     */
    CLIENT_SESSION_TRACK(1 << 23),
    /**
     * EOF_Packet deprecation :<br/>
     * OK_Packet replace EOF_Packet in end of Resulset when in text format<br/>
     * EOF_Packet between columns definition and resultsetRows is deleted
     */
    CLIENT_DEPRECATE_EOF(1 << 24),
    /**
     * 	Support zstd protocol compression
     */
    CLIENT_ZSTD_COMPRESSION_ALGORITHM(1 << 26),
    /**
     * 	reserved for futur use. (Was CLIENT_PROGRESS Client support progress indicator before 10.2)
     */
    CLIENT_CAPABILITY_EXTENSION(1 << 29),
    ;

    private int value;

    CapabilityFlag(int value) {
        this.value = value;
    }

    public boolean support(int capabilities) {
        return (capabilities & this.value) != 0;
    }

    public int getValue() {
        return value;
    }
}
