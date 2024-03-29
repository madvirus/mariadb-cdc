package mariadbcdc.binlog.reader.packet.result;

import mariadbcdc.binlog.reader.io.ColumnDefPacket;
import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.ReadPacket;

public class TextResultSetRowPacket implements ReadPacket {
    private int sequenceNumber;
    private String[] values;

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public String[] getValues() {
        return values;
    }

    public static TextResultSetRowPacket from(ColumnDefPacket[] defs, ReadPacketData readPacketData) {
        TextResultSetRowPacket packet = new TextResultSetRowPacket();
        packet.values = new String[defs.length];
        for (int i = 0 ; i < defs.length ; i++) {
            packet.values[i] = readPacketData.readLengthEncodedString();
        }
        return packet;
    }
}
