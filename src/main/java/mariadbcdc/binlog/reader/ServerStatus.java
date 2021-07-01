package mariadbcdc.binlog.reader;

public enum ServerStatus {

    /**
     * A transaction is currently active
     */
    SERVER_STATUS_IN_TRANS(1),
    /**
     * Autocommit mode is set
     */
    SERVER_STATUS_AUTOCOMMIT(2),
    /**
     * more results exists (more packet follow)
     */
    SERVER_MORE_RESULTS_EXISTS(8),
    SERVER_QUERY_NO_GOOD_INDEX_USED(16),
    SERVER_QUERY_NO_INDEX_USED(32),
    /**
     * when using COM_STMT_FETCH, indicate that current cursor still has result
     */
    SERVER_STATUS_CURSOR_EXISTS(64),
    /**
     * when using COM_STMT_FETCH, indicate that current cursor has finished to send results
     */
    SERVER_STATUS_LAST_ROW_SENT(128),
    /**
     * database has been dropped
     */
    SERVER_STATUS_DB_DROPPED(1 << 8),
    /**
     * current escape mode is "no backslash escape"
     */
    SERVER_STATUS_NO_BACKSLASH_ESCAPES(1 << 9),
    /**
     * A DDL change did have an impact on an existing PREPARE (an automatic reprepare has been executed)
     */
    SERVER_STATUS_METADATA_CHANGED(1 << 10),
    SERVER_QUERY_WAS_SLOW(1 << 11),
    /**
     * this resultset contain stored procedure output parameter
     */
    SERVER_PS_OUT_PARAMS(1 << 12),
    /**
     * current transaction is a read-only transaction
     */
    SERVER_STATUS_IN_TRANS_READONLY(1 << 13),
    /**
     * session state change. see Session change type for more information
     */
    SERVER_SESSION_STATE_CHANGED(1 << 14),
    ;

    private int value;

    ServerStatus(int value) {
        this.value = value;
    }

    public boolean contains(int status) {
        return (status & this.value) != 0;
    }

    public int getValue() {
        return value;
    }
}
