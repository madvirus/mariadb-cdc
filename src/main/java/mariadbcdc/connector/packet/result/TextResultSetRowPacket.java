package mariadbcdc.connector.packet.result;

import mariadbcdc.connector.io.ColumnDefPacket;
import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.ReadPacket;

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
