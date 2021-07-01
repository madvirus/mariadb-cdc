package mariadbcdc.binlog.packet.binlog.des;

import mariadbcdc.binlog.io.ReadPacketData;
import mariadbcdc.binlog.packet.binlog.BinLogData;
import mariadbcdc.binlog.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.packet.binlog.BinLogStatus;
import mariadbcdc.binlog.packet.binlog.data.RotateEvent;
import mariadbcdc.binlog.packet.binlog.data.TableMapEvent;

import java.util.Map;

public class RotateEventBinLogDataDeserializer implements BinLogDataDeserializer {

    @Override
    public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        RotateEvent rotateEvent = new RotateEvent(readPacketData.readLong(8), readPacketData.readStringEOF());
        return rotateEvent;
    }
}
