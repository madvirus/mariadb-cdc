package mariadbcdc.connector.packet.connection;

import mariadbcdc.connector.io.ByteWriter;
import mariadbcdc.connector.CapabilityFlag;
import mariadbcdc.connector.Collation;
import mariadbcdc.connector.MariadbPassword;
import mariadbcdc.connector.packet.WritePacket;

import java.math.BigInteger;

public class HandshakeResponsePacket implements WritePacket {
    private int sequenceNumber;
    private String seed;
    private int clientCapabilities;
    private int maxPacketSize;
    private int clientCharacterCollation;
    // String reserved // 19
    // int extendedClientCapabilities // 4
    private String username; // string<NUL>
    private byte[] authdata;
    private String authenticationPluginName;

    private boolean authLenencClientData;
    private boolean secureConnection;
    private boolean connectWithDb;
    private boolean pluginAuth;
    private boolean connectAttrs;

    public HandshakeResponsePacket(int clientCapabilities,
                                   int serverCapability, int sequenceNumber,
                                   String user, String password, String seed,
                                   String authenticationPluginName) {
        this.sequenceNumber = sequenceNumber;
        this.seed = seed;
        this.clientCapabilities = clientCapabilities;
        this.maxPacketSize = 1024 * 1024 * 1024; // 1G
        this.clientCharacterCollation = Collation.utf8.getId();
        this.username = user;
        this.authdata = MariadbPassword.nativePassword(password, seed);
        this.authenticationPluginName = authenticationPluginName;
        authLenencClientData = CapabilityFlag.PLUGIN_AUTH_LENENC_CLIENT_DATA.support(serverCapability);
        secureConnection = CapabilityFlag.SECURE_CONNECTION.support(serverCapability);
        connectWithDb = CapabilityFlag.CONNECT_WITH_DB.support(clientCapabilities);
        pluginAuth = CapabilityFlag.PLUGIN_AUTH.support(serverCapability);
        connectAttrs = CapabilityFlag.CONNECT_ATTRS.support(serverCapability);
    }

    @Override
    public void writeTo(ByteWriter writer) {
        writer.sequenceNumber(sequenceNumber);
        writer.write(clientCapabilities, 4);
        writer.write(maxPacketSize, 4);
        writer.write(clientCharacterCollation, 1);
        writer.reserved(19);
        writer.reserved(4); // maria extended flag 없음
        writer.writeStringNul(username);
        if (authLenencClientData) {
            writer.writeBytesLenenc(authdata);
        } else if (secureConnection) {
            writer.write(authdata.length, 1);
            writer.writeBytes(authdata);
        } else {
            writer.writeBytes(authdata);
            writer.writeZero();
        }
        if (connectWithDb) {
            writer.writeStringNul("test");
        }
        if (pluginAuth) {
            writer.writeStringNul(authenticationPluginName);
        }
        if (connectAttrs) {
            writer.writeEncodedLength(0);
            // TODO 옵션 전송
        }
    }

    @Override
    public String toString() {
        return "HandshakeResponsePacket{" +
                "sequenceNumber=" + sequenceNumber +
                ", seed='" + seed + '\'' +
                ", clientCapabilities=" + clientCapabilities +
                ", maxPacketSize=" + maxPacketSize +
                ", clientCharacterCollation=" + clientCharacterCollation +
                ", username='" + username + '\'' +
                ", authdata='" + new BigInteger(1, authdata).toString(16) +
                ", authenticationPluginName='" + authenticationPluginName + '\'' +
                ", authLenencClientData=" + authLenencClientData +
                ", secureConnection=" + secureConnection +
                ", connectWithDb=" + connectWithDb +
                ", pluginAuth=" + pluginAuth +
                ", connectAttrs=" + connectAttrs +
                '}';
    }
}
