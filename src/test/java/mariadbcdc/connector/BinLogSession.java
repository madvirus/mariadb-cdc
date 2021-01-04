package mariadbcdc.connector;

import mariadbcdc.connector.handler.BinLogHandler;
import mariadbcdc.connector.handler.HandshakeHandler;
import mariadbcdc.connector.handler.HandshakeSuccessResult;
import mariadbcdc.connector.handler.RegisterSlaveHandler;
import mariadbcdc.connector.io.Either;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.ComQuitPacket;
import mariadbcdc.connector.packet.OkPacket;
import mariadbcdc.connector.packet.query.ComQueryPacket;
import mariadbcdc.connector.packet.result.ResultSetPacket;
import mariadbcdc.connector.packet.result.ResultSetRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

class BinLogSession {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final Socket socket;
    private final InputStream is;
    private final OutputStream out;
    private final String user;
    private final String password;

    private PacketIO packetIO;

    private String binlogFile;
    private long binlogPosition;

    private int serverCapabilities;
    private int clientCapabilities;
    private int connectionId;
    private Long masterServerId;
    private long slaveServerId = 65534;

    private BinLogHandler binLogHandler;

    public BinLogSession(String host, int port, String user, String password) {
        this.user = user;
        this.password = password;
        try {
            socket = new Socket(host, port);
            is = new BufferedInputStream(socket.getInputStream());
            out = new BufferedOutputStream(socket.getOutputStream());
            packetIO = new PacketIO(is, out);
        } catch (IOException e) {
            throw new BinLogException(e);
        }
    }

    public void handshake() {
        HandshakeSuccessResult handshake = new HandshakeHandler(user, password, packetIO).handshake();
        connectionId = handshake.getConnectionId();
        this.clientCapabilities = handshake.getClientCapabilities();
        this.serverCapabilities = handshake.getServerCapabilities();
    }

    public void fetchBinlogFilenameAndPosition() {
        packetIO.write(new ComQueryPacket("show master status"));
        Either<OkPacket,ResultSetPacket> readPacket = packetIO.readResultSetPacket();
        ResultSetPacket rsp = readPacket.getRight();
        if (rsp.getRows().isEmpty()) {
            throw new BinLogException("Failed to read binlog filename/position");
        }
        ResultSetRow row0 = rsp.getRows().get(0);
        this.binlogFile = row0.getString(0);
        this.binlogPosition = row0.getLong(1);
        if (binlogPosition < 4) {
            this.binlogPosition = 4;
        }
        logger.info("binlog filename/position: {}/{}", binlogFile, binlogPosition);
    }

    public void registerSlave() {
        RegisterSlaveHandler handler = new RegisterSlaveHandler(packetIO);
        this.masterServerId = handler.getServerId();
        logger.info("serverId: {}", masterServerId);
        String checksum = handler.handleChecksum();
        logger.info("checksum: {}", checksum);

        binLogHandler = new BinLogHandler(packetIO, checksum);

        handler.startBinlogDump(binlogFile, binlogPosition, slaveServerId);
    }

    public void readBinlog() {
        binLogHandler.readBinLogEvent();
    }

    public String getBinlogFile() {
        return binlogFile;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public void close() {
        logger.info("closing session");
        try {
            packetIO.write(ComQuitPacket.INSTANCE);
        } catch (Exception e) {
        }
        try {
            is.close();
        } catch (IOException e) {
        }
        try {
            out.close();
        } catch (IOException e) {
        }
        try {
            socket.close();
        } catch (IOException e) {
        }
        logger.info("closed session");
    }
}
