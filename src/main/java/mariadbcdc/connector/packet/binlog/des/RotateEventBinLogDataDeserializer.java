package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;
import mariadbcdc.connector.packet.binlog.data.RotateEvent;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;

import java.util.Map;

public class RotateEventBinLogDataDeserializer implements BinLogDataDeserializer {

    @Override
    public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        RotateEvent rotateEvent = new RotateEvent(readPacketData.readLong(8), readPacketData.readStringEOF());
        return rotateEvent;
    }
}
