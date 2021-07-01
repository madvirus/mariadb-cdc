package mariadbcdc.binlog.reader.packet;

import mariadbcdc.binlog.reader.CapabilityFlag;
import mariadbcdc.binlog.reader.ServerStatus;
import mariadbcdc.binlog.reader.io.ReadPacketData;

public class OkPacket implements ReadPacket {
    private int sequenceNumber;
    private int header;
    private long affectedRows;
    private long lastInsertId;
    private int serverStatus;
    private int warningCount;
    private String info;
    private String sessionStateInfo;
    private String variable;

    public static OkPacket from(ReadPacketData readPacketData, int clientCapabilities) {
        OkPacket okPacket = new OkPacket();
        okPacket.sequenceNumber = readPacketData.getSequenceNumber();
        okPacket.header = readPacketData.readInt(1);
        okPacket.affectedRows = readPacketData.readLengthEncodedInt();
        okPacket.lastInsertId = readPacketData.readLengthEncodedLong();
        okPacket.serverStatus = readPacketData.readInt(2);
        okPacket.warningCount = readPacketData.readInt(2);
        if (CapabilityFlag.CLIENT_SESSION_TRACK.support(clientCapabilities)) {
            okPacket.info = readPacketData.readLengthEncodedString();
            if (ServerStatus.SERVER_SESSION_STATE_CHANGED.contains(okPacket.serverStatus)) {
                okPacket.sessionStateInfo = readPacketData.readLengthEncodedString();
                okPacket.variable = readPacketData.readLengthEncodedString();
            }
        } else {
            okPacket.info = readPacketData.readStringEOF();
        }
        return okPacket;
    }

    @Override
    public String toString() {
        return "OkPacket{" +
                "sequenceNumber=" + sequenceNumber +
                ", header=" + header +
                ", affectedRows=" + affectedRows +
                ", lastInsertId=" + lastInsertId +
                ", serverStatus=" + serverStatus +
                ", warningCount=" + warningCount +
                ", info='" + info + '\'' +
                ", sessionStateInfo='" + sessionStateInfo + '\'' +
                ", variable='" + variable + '\'' +
                '}';
    }
}
