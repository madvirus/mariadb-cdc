package mariadbcdc.binlog.reader.packet.connection;

import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.ReadPacket;

import java.util.Arrays;

public class AuthSwitchRequestPacket implements ReadPacket {
    private int sequenceNumber;
    private int header;
    private String authPluginName;
    private byte[] authPluginData;

    public static AuthSwitchRequestPacket from(ReadPacketData readPacketData) {
        AuthSwitchRequestPacket packet = new AuthSwitchRequestPacket();
        packet.sequenceNumber = readPacketData.getSequenceNumber();
        packet.header = readPacketData.readInt(1);
        packet.authPluginName = readPacketData.readStringNul();
        packet.authPluginData = readPacketData.readBytesEof();
        return packet;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public int getHeader() {
        return header;
    }

    public String getAuthPluginName() {
        return authPluginName;
    }

    public byte[] getAuthPluginData() {
        return authPluginData;
    }

    @Override
    public String toString() {
        return "AuthSwitchRequestPacket{" +
                "sequenceNumber=" + sequenceNumber +
                ", header=" + header +
                ", authPluginName='" + authPluginName + '\'' +
                ", authPluginData=" + Arrays.toString(authPluginData) +
                '}';
    }
}
