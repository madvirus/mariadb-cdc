package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;

import java.util.Map;

public interface BinLogDataDeserializer {
    BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap);
}
