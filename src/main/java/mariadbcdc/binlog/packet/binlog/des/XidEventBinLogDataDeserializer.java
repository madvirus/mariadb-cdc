package mariadbcdc.binlog.packet.binlog.des;

import mariadbcdc.binlog.io.ReadPacketData;
import mariadbcdc.binlog.packet.binlog.BinLogData;
import mariadbcdc.binlog.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.packet.binlog.BinLogStatus;
import mariadbcdc.binlog.packet.binlog.data.TableMapEvent;
import mariadbcdc.binlog.packet.binlog.data.XidEvent;

import java.util.Map;

public class XidEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        return new XidEvent(readPacketData.readLong(8));
    }
}
