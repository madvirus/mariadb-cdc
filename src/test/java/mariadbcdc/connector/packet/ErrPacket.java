package mariadbcdc.connector.packet;

import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.binlog.BinLogStatus;

public class ErrPacket implements ReadPacket {
    private int sequenceNumber;
    private int header;
    private int errorCode;
    private int stage;
    private int maxStage;
    private int progress;
    private String progressInfo;
    private String sqlStateMarker;
    private String sqlState;
    private String errorMessage;

    public static ErrPacket from(ReadPacketData readPacketData) {
        ErrPacket errPacket = new ErrPacket();
        errPacket.sequenceNumber = readPacketData.getSequenceNumber();
        errPacket.header = readPacketData.readInt(1);
        errPacket.errorCode = readPacketData.readInt(2);
        if (errPacket.errorCode == 0xFFFF) {
            errPacket.stage = readPacketData.readInt(1);
            errPacket.maxStage = readPacketData.readInt(1);
            errPacket.progress = readPacketData.readInt(3);
            errPacket.progressInfo = readPacketData.readLengthEncodedString();
        } else {
            if (readPacketData.readInt(1) == '#') {
                errPacket.sqlStateMarker = "#";
                errPacket.sqlState = readPacketData.readString(5);
                errPacket.errorMessage = readPacketData.readStringEOF();
            } else {
                readPacketData.rewind(1);
                errPacket.errorMessage = readPacketData.readStringEOF();
            }
        }
        return errPacket;
    }

    public static ErrPacket from(BinLogStatus binLogStatus, PacketIO packetIO) {
        ErrPacket errPacket = new ErrPacket();
        errPacket.sequenceNumber = binLogStatus.getSeq();
        errPacket.header = binLogStatus.getStatus();
        packetIO.startBlock(binLogStatus.getLength() - 1); // -1은 header 값

        errPacket.errorCode = packetIO.readInt(2);
        if (errPacket.errorCode == 0xFFFF) {
            errPacket.stage = packetIO.readInt(1);
            errPacket.maxStage = packetIO.readInt(1);
            errPacket.progress = packetIO.readInt(3);
            errPacket.progressInfo = packetIO.readLengthEncodedString();
        } else {
            int ch = packetIO.readInt(1);
            if (ch == '#') {
                errPacket.sqlStateMarker = "#";
                errPacket.sqlState = packetIO.readString(5);
                errPacket.errorMessage = packetIO.readStringEOF();
            } else {
                errPacket.errorMessage = ((char)ch) + packetIO.readStringEOF();
            }
        }
        return errPacket;
    }

    @Override
    public String toString() {
        return "ErrPacket{" +
                "sequenceNumber=" + sequenceNumber +
                ", header=" + header +
                ", errorCode=" + errorCode +
                ", stage=" + stage +
                ", maxStage=" + maxStage +
                ", progress=" + progress +
                ", progressInfo='" + progressInfo + '\'' +
                ", sqlStateMarker='" + sqlStateMarker + '\'' +
                ", sqlState='" + sqlState + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
