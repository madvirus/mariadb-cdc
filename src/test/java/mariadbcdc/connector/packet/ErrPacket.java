package mariadbcdc.connector.packet;

import mariadbcdc.connector.io.ReadPacketData;

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
