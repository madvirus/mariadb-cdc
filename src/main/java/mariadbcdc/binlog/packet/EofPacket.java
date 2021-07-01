package mariadbcdc.binlog.packet;

import mariadbcdc.binlog.io.ReadPacketData;

public class EofPacket implements ReadPacket {
    private int sequenceNumber;
    private int header;
    private int warningCount;
    private int serverStatus;

    public static EofPacket from(ReadPacketData readPacketData) {
        EofPacket eofPacket = new EofPacket();
        eofPacket.sequenceNumber = readPacketData.getSequenceNumber();
        eofPacket.header = readPacketData.readInt(1);
        eofPacket.warningCount = readPacketData.readInt(2);
        eofPacket.serverStatus = readPacketData.readInt(2);
        return eofPacket;
    }

    @Override
    public String toString() {
        return "EofPacket{" +
                "sequenceNumber=" + sequenceNumber +
                ", header=" + header +
                ", warningCount=" + warningCount +
                ", serverStatus=" + serverStatus +
                '}';
    }
}
