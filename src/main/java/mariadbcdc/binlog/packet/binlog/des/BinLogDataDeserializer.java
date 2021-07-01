package mariadbcdc.binlog.packet.binlog.des;

import mariadbcdc.binlog.io.ReadPacketData;
import mariadbcdc.binlog.packet.binlog.BinLogData;
import mariadbcdc.binlog.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.packet.binlog.BinLogStatus;
import mariadbcdc.binlog.packet.binlog.data.TableMapEvent;

import java.util.Map;

public interface BinLogDataDeserializer {
    BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap);
}
