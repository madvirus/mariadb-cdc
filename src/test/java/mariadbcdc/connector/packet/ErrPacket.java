package mariadbcdc.connector.packet;

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

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public int getHeader() {
        return header;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getStage() {
        return stage;
    }

    public int getMaxStage() {
        return maxStage;
    }

    public int getProgress() {
        return progress;
    }

    public String getProgressInfo() {
        return progressInfo;
    }

    public String getSqlStateMarker() {
        return sqlStateMarker;
    }

    public String getSqlState() {
        return sqlState;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

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

    public static ErrPacket from(BinLogStatus binLogStatus, ReadPacketData readPacketData) {
        ErrPacket errPacket = new ErrPacket();
        errPacket.sequenceNumber = binLogStatus.getSeq();
        errPacket.header = binLogStatus.getStatus();

        errPacket.errorCode = readPacketData.readInt(2);
        if (errPacket.errorCode == 0xFFFF) {
            errPacket.stage = readPacketData.readInt(1);
            errPacket.maxStage = readPacketData.readInt(1);
            errPacket.progress = readPacketData.readInt(3);
            errPacket.progressInfo = readPacketData.readLengthEncodedString();
        } else {
            int ch = readPacketData.readInt(1);
            if (ch == '#') {
                errPacket.sqlStateMarker = "#";
                errPacket.sqlState = readPacketData.readString(5);
                errPacket.errorMessage = readPacketData.readStringEOF();
            } else {
                errPacket.errorMessage = ((char)ch) + readPacketData.readStringEOF();
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
