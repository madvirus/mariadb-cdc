package mariadbcdc.binlog.reader.packet.binlog.des;

import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.reader.packet.binlog.BinLogStatus;
import mariadbcdc.binlog.reader.packet.binlog.BinlogEventType;
import mariadbcdc.binlog.reader.packet.binlog.data.TableMapEvent;

import java.util.HashMap;
import java.util.Map;

public class BinLogDataDeserializers {
    private static Map<BinlogEventType, BinLogDataDeserializer> map = new HashMap<>();
    private static BinLogDataDeserializer NULL = new NullBinLogDataDeserializer();

    static {
        map.put(BinlogEventType.ROTATE_EVENT, new RotateEventBinLogDataDeserializer());
        map.put(BinlogEventType.FORMAT_DESCRIPTION_EVENT, new FormatDescriptionEventBinLogDataDeserializer());
        map.put(BinlogEventType.QUERY_EVENT, new QueryEventBinLogDataDeserializer());
        map.put(BinlogEventType.TABLE_MAP_EVENT, new TableMapEventBinLogDataDeserializer());
        map.put(BinlogEventType.WRITE_ROWS_EVENT_V1, new WriteRowsEventBinLogDataDeserializer());
        map.put(BinlogEventType.UPDATE_ROWS_EVENT_V1, new UpdateRowsEventBinLogDataDeserializer());
        map.put(BinlogEventType.DELETE_ROWS_EVENT_V1, new DeleteRowsEventBinLogDataDeserializer());
        map.put(BinlogEventType.XID_EVENT, new XidEventBinLogDataDeserializer());
        map.put(BinlogEventType.HEARTBEAT_LOG_EVENT, new HeartbeatEventBinLogDataDeserializer());
        map.put(BinlogEventType.STOP_EVENT, new StopEventBinLogDataDeserializer());
    }

    public static BinLogDataDeserializer getDeserializer(BinlogEventType eventType) {
        return map.get(eventType);
    }

    private static class NullBinLogDataDeserializer implements BinLogDataDeserializer {
        @Override
        public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
            return null;
        }
    }
}
