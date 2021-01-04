package mariadbcdc.connector.binlog;

import java.util.HashMap;
import java.util.Map;

public enum BinlogEventType {
    UNKNOWN(0x00),
    QUERY_EVENT(0x02),
    STOP_EVENT(0x03),
    ROTATE_EVENT(0x04),
    XID_EVENT(0x10),
    RAND_EVENT(0x0d),
    USER_VAR_EVENT(0x0e),
    FORMAT_DESCRIPTION_EVENT(0x0f),
    TABLE_MAP_EVENT(0x13),
    HEARTBEAT_LOG_EVENT(0x1b),
    ANNOTATE_ROWS_EVENT(0xa0),
    BINLOG_CHECKPOINT_EVENT(0xa1),
    GTID_EVENT(0xa2),
    GTID_LIST_EVENT(0xa3),
    START_ENCRYPTION_EVENT(0xa4),
    ;


    private final int code;

    BinlogEventType(int code) {
        this.code = code;
    }

    private static Map<Integer, BinlogEventType> codeMap = new HashMap<>();
    static {
        for (BinlogEventType value : values()) {
            codeMap.put(value.code, value);
        }
    }

    public static BinlogEventType byCode(int code) {
        return codeMap.getOrDefault(code, UNKNOWN);
    }
}
