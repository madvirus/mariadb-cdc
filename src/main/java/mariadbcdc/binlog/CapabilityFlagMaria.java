package mariadbcdc.binlog;

public enum CapabilityFlagMaria {
    /**
     * 	Client support progress indicator (since 10.2)
     */
    MARIADB_CLIENT_PROGRESS(1L << 32),
    /**
     * Permit COM_MULTI protocol
     */
    MARIADB_CLIENT_COM_MULTI(1L << 33),
    /**
     * Permit bulk insert
     */
    MARIADB_CLIENT_STMT_BULK_OPERATIONS(1L << 34),
    /**
     * add extended metadata information
     */
    MARIADB_CLIENT_EXTENDED_TYPE_INFO(1L << 35)
    ;

    private long value;

    CapabilityFlagMaria(long value) {
        this.value = value;
    }

    public boolean support(long capabilities) {
        return (capabilities & this.value) != 0;
    }

    public long getValue() {
        return value;
    }
}
