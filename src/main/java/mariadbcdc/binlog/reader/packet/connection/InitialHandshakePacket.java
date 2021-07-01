package mariadbcdc.binlog.reader.packet.connection;

import mariadbcdc.binlog.reader.CapabilityFlag;
import mariadbcdc.binlog.reader.Collation;
import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.ReadPacket;

import java.util.ArrayList;
import java.util.List;

public class InitialHandshakePacket implements ReadPacket {
    private int sequenceNumber;
    private int protocolVersion;
    private String serverVersion;
    private int connectionId;
    private String scramble1;
    private String reservedByte;
    private int serverCapabilities1;
    private int serverDefaultCollation;
    private int statusFlags;
    private int serverCapabilities2;
    private int pluginDataLength;
    private String filter;
    private int serverCapabilities3;
    private String scramble2;
    private String reservedByte2;
    private String authenticationPluginName;

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public String getScramble1() {
        return scramble1;
    }

    public String getReservedByte() {
        return reservedByte;
    }

    public int getServerCapabilities1() {
        return serverCapabilities1;
    }

    public int getServerDefaultCollation() {
        return serverDefaultCollation;
    }

    public int getStatusFlags() {
        return statusFlags;
    }

    public int getServerCapabilities2() {
        return serverCapabilities2;
    }

    public int getPluginDataLength() {
        return pluginDataLength;
    }

    public String getFilter() {
        return filter;
    }

    public int getServerCapabilities3() {
        return serverCapabilities3;
    }

    public String getScramble2() {
        return scramble2;
    }

    public String getReservedByte2() {
        return reservedByte2;
    }

    public String getAuthenticationPluginName() {
        return authenticationPluginName;
    }

    public String getSeed() {
        return scramble1 + scramble2;
    }

    public int getServerCapabilities() {
        return serverCapabilities1 + serverCapabilities2;
    }

    public static InitialHandshakePacket from(ReadPacketData readPacketData) {
        InitialHandshakePacket initialPacket = new InitialHandshakePacket();
        initialPacket.sequenceNumber = readPacketData.getSequenceNumber();
        initialPacket.protocolVersion = readPacketData.readInt(1);
        initialPacket.serverVersion = readPacketData.readStringNul();
        initialPacket.connectionId = readPacketData.readInt(4);
        initialPacket.scramble1 = readPacketData.readString(8); // scramble 1st part (authentication seed)
        initialPacket.reservedByte = readPacketData.readString(1);
        initialPacket.serverCapabilities1 = readPacketData.readInt(2);
        initialPacket.serverDefaultCollation = readPacketData.readInt(1);
        initialPacket.statusFlags = readPacketData.readInt(2);
        initialPacket.serverCapabilities2 = readPacketData.readInt(2) << 16;
        if (CapabilityFlag.PLUGIN_AUTH.support(initialPacket.serverCapabilities2)) {
            initialPacket.pluginDataLength = readPacketData.readInt(1);
        } else {
            readPacketData.readInt(1);
        }
        initialPacket.filter = readPacketData.readString(6);
        initialPacket.serverCapabilities3 = readPacketData.readInt(4);
        if (CapabilityFlag.SECURE_CONNECTION.support(initialPacket.serverCapabilities1)) {
            initialPacket.scramble2 = readPacketData.readString(Math.max(12, initialPacket.pluginDataLength - 9));
            initialPacket.reservedByte2 = readPacketData.readString(1);
        }
        if (CapabilityFlag.PLUGIN_AUTH.support(initialPacket.serverCapabilities2)) {
            initialPacket.authenticationPluginName = readPacketData.readStringNul();
        }
        return initialPacket;
    }

    public List<CapabilityFlag> getAllCapabilities() {
        ArrayList<CapabilityFlag> flags = new ArrayList<>();
        for (CapabilityFlag flag : CapabilityFlag.values()) {
            if (flag.support(serverCapabilities1) ||
                    flag.support(serverCapabilities2)) {
                flags.add(flag);
            }
        }
        return flags;
    }

    @Override
    public String toString() {
        return "InitialPacket{" +
                "sequenceNumber=" + sequenceNumber +
                ", protocolVersion=" + protocolVersion +
                ", serverVersion='" + serverVersion + '\'' +
                ", connectionId=" + connectionId +
                ", scramble1='" + scramble1 + '\'' +
                ", reservedByte='" + reservedByte + '\'' +
                ", serverCapabilities1=" + Integer.toBinaryString(serverCapabilities1) +
                ", serverDefaultCollation=" + serverDefaultCollation + "(" + Collation.byId(serverDefaultCollation) + ")" +
                ", statusFlags=" + statusFlags +
                ", serverCapabilities2=" + Integer.toBinaryString(serverCapabilities2) +
                ", pluginDataLength=" + pluginDataLength +
                ", filter='" + filter + '\'' +
                ", serverCapabilities3=" + Integer.toBinaryString(serverCapabilities3) +
                ", scramble2='" + scramble2 + '\'' +
                ", reservedByte2='" + reservedByte2 + '\'' +
                ", tauthenticationPluginName='" + authenticationPluginName + '\'' +
                '}';
    }
}
