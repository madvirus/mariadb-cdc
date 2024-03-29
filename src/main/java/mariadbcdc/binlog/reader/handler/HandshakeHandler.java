package mariadbcdc.binlog.reader.handler;

import mariadbcdc.binlog.reader.BinLogBadPacketException;
import mariadbcdc.binlog.reader.CapabilityFlag;
import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.*;
import mariadbcdc.binlog.reader.packet.connection.AuthSwitchRequestPacket;
import mariadbcdc.binlog.reader.packet.connection.AuthSwitchResponsePacket;
import mariadbcdc.binlog.reader.packet.connection.HandshakeResponsePacket;
import mariadbcdc.binlog.reader.packet.connection.InitialHandshakePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandshakeHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final String user;
    private final String password;

    private InitialHandshakePacket packet;
    private int serverCapabilities;
    private int clientCapabilities;

    private ReadPacketReader reader;
    private WritePacketWriter writer;

    public HandshakeHandler(String user,
                            String password,
                            ReadPacketReader readPacketReader,
                            WritePacketWriter writer) {
        this.user = user;
        this.password = password;
        this.reader = readPacketReader;
        this.writer = writer;
    }

    public HandshakeSuccessResult handshake() {
        this.packet = receiveInitialHandshakePacket();
        logger.debug("received initial handshake packet: {}", packet);
        serverCapabilities = packet.getServerCapabilities();

        HandshakeResponsePacket respPacket = createHandshakeResponsePacket(packet);
        writer.write(respPacket);
        logger.debug("sended handshake response packet: {}", respPacket);

        ReadPacket responsePacket = receiveServerResponse();
        if (responsePacket instanceof OkPacket) {
            logger.debug("handshake OK: {}", responsePacket);
            return createHandshakeSuccessResult();
        } else if (responsePacket instanceof ErrPacket) {
            logger.debug("handshake FAIL: {}", responsePacket);
            throw new BinLogHandshakeFailException();
        } else if (responsePacket instanceof AuthSwitchRequestPacket) {
            handleAuthSwitch(responsePacket);
            return createHandshakeSuccessResult();
        } else {
            throw new BinLogBadPacketException(String.format("bad handshake response packet: ", responsePacket));
        }
    }

    private HandshakeSuccessResult createHandshakeSuccessResult() {
        return new HandshakeSuccessResult(
                packet.getProtocolVersion(),
                packet.getServerVersion(),
                packet.getConnectionId(),
                serverCapabilities,
                clientCapabilities
        );
    }

    private void handleAuthSwitch(ReadPacket responsePacket) {
        logger.debug("receive AuthSwitchRequestPacket: {}", responsePacket);
        AuthSwitchRequestPacket authSwitchReq = (AuthSwitchRequestPacket) responsePacket;
        if (!"mysql_native_password".equals(authSwitchReq.getAuthPluginName())) {
            throw new UnsupportedAuthPluginException(authSwitchReq.getAuthPluginName());
        }
        AuthSwitchResponsePacket authRespPacket = createAuthSwitchResponsePacket(authSwitchReq);
        writer.write(authRespPacket);
        logger.debug("sended auth switch response packet: {}", authRespPacket);

        ReadPacket switchRespPacket = receiveServerResponse();
        if (switchRespPacket instanceof OkPacket) {
            logger.debug("handshake OK: {}", switchRespPacket);
        } else if (switchRespPacket instanceof ErrPacket) {
            logger.debug("handshake FAIL: {}", switchRespPacket);
            throw new BinLogHandshakeFailException();
        } else {
            throw new BinLogBadPacketException(String.format("bad handshake response packet: ", responsePacket));
        }
    }

    private InitialHandshakePacket receiveInitialHandshakePacket() {
        ReadPacketData readPacketData = reader.readPacketData();
        InitialHandshakePacket packet = InitialHandshakePacket.from(readPacketData);
        return packet;
    }

    private HandshakeResponsePacket createHandshakeResponsePacket(InitialHandshakePacket packet) {
        // AbstractConnectProtocol#initializeClientCapabilities 참고
        clientCapabilities = CapabilityFlag.IGNORE_SPACE.getValue()
                | CapabilityFlag.CLIENT_PROTOCOL_41.getValue()
                | CapabilityFlag.TRANSACTIONS.getValue()
                // | CapabilityFlag.SECURE_CONNECTION.getValue()
                | CapabilityFlag.MULTI_RESULTS.getValue()
                | CapabilityFlag.PS_MULTI_RESULTS.getValue()
                | CapabilityFlag.CONNECT_ATTRS.getValue()
                | CapabilityFlag.PLUGIN_AUTH_LENENC_CLIENT_DATA.getValue()
                //| CapabilityFlag.CLIENT_SESSION_TRACK.getValue()
                | CapabilityFlag.MULTI_STATEMENTS.getValue()
                ;

        if (CapabilityFlag.PLUGIN_AUTH.support(serverCapabilities)) {
            clientCapabilities |= CapabilityFlag.PLUGIN_AUTH.getValue();
        }
        if (CapabilityFlag.CLIENT_DEPRECATE_EOF.support(serverCapabilities)) {
            clientCapabilities |= CapabilityFlag.CLIENT_DEPRECATE_EOF.getValue();
        }

        // | CapabilityFlag.CONNECT_WITH_DB.getValue()
        return new HandshakeResponsePacket(
                clientCapabilities,
                serverCapabilities,
                packet.getSequenceNumber() + 1,
                user, password,
                packet.getSeed(),
                packet.getAuthenticationPluginName());
    }

    private AuthSwitchResponsePacket createAuthSwitchResponsePacket(AuthSwitchRequestPacket authSwitchReq) {
        return new AuthSwitchResponsePacket(
                authSwitchReq.getSequenceNumber() + 1,
                password,
                authSwitchReq.getAuthPluginData()
        );
    }


    private ReadPacket receiveServerResponse() {
        return reader.read(clientCapabilities);
    }

}
