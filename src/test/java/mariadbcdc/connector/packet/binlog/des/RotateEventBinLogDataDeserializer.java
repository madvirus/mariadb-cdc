package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;
import mariadbcdc.connector.packet.binlog.data.RotateEvent;

public class RotateEventBinLogDataDeserializer implements BinLogDataDeserializer {

    @Override
    public BinLogData deserialize(PacketIO packetIO, BinLogStatus binLogStatus, BinLogHeader header) {
        RotateEvent rotateEvent = new RotateEvent(packetIO.readLong(8), packetIO.readStringEOF());
        return rotateEvent;
    }
}
