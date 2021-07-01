package mariadbcdc.binlog.reader.packet.result;

import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.ReadPacket;

public class ColumnCountPacket implements ReadPacket {
    private int sequenceNumber;
    private int count;

    public static ColumnCountPacket from(ReadPacketData readPacketData) {
        return new ColumnCountPacket(readPacketData.getSequenceNumber(), readPacketData.readLengthEncodedInt());
    }

    public ColumnCountPacket(int sequenceNumber, int count) {
        this.sequenceNumber = sequenceNumber;
        this.count = count;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "ColumnCountPacket{" +
                "sequenceNumber=" + sequenceNumber +
                ", count=" + count +
                '}';
    }
}
