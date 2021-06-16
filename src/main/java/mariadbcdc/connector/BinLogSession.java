package mariadbcdc.connector;

import mariadbcdc.BinlogPosition;
import mariadbcdc.connector.handler.BinLogHandler;
import mariadbcdc.connector.handler.HandshakeHandler;
import mariadbcdc.connector.handler.HandshakeSuccessResult;
import mariadbcdc.connector.handler.RegisterSlaveHandler;
import mariadbcdc.connector.io.Either;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.*;
import mariadbcdc.connector.packet.binlog.BinLogEvent;
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

    private int serverCapabilities;
    private int clientCapabilities;
    private int connectionId;
    private Long masterServerId;

    private String checksum;

    private BinLogHandler binLogHandler;

    private final ReadPacketReader readPacketReader;
    private final WritePacketWriter writePacketWriter;

    public BinLogSession(String host, int port, String user, String password) {
        this.user = user;
        this.password = password;
        try {
            socket = new Socket(host, port);
            is = new BufferedInputStream(socket.getInputStream());
            out = new BufferedOutputStream(socket.getOutputStream());
            packetIO = new PacketIO(is, out);
            readPacketReader = new ReadPacketReader(packetIO);
            writePacketWriter = new WritePacketWriter(packetIO);
        } catch (IOException e) {
            throw new BinLogException(e);
        }
    }

    public void handshake() {
        HandshakeSuccessResult handshake = new HandshakeHandler(user, password, readPacketReader, writePacketWriter).handshake();
        connectionId = handshake.getConnectionId();
        this.clientCapabilities = handshake.getClientCapabilities();
        this.serverCapabilities = handshake.getServerCapabilities();
    }

    public BinlogPosition fetchBinlogFilePosition() {
        writePacketWriter.write(new ComQueryPacket("show master status"));
        Either<OkPacket, ResultSetPacket> readPacket = new ReadPacketReader(packetIO).readResultSetPacket(clientCapabilities);
        ResultSetPacket rsp = readPacket.getRight();
        if (rsp.getRows().isEmpty()) {
            throw new BinLogException("Failed to read binlog filename/position");
        }
        ResultSetRow row0 = rsp.getRows().get(0);
        String binlogFile = row0.getString(0);
        long binlogPosition = row0.getLong(1);
        if (binlogPosition < 4) {
            binlogPosition = 4;
        }
        logger.info("fetch binlog filename/position: {}/{}", binlogFile, binlogPosition);
        return new BinlogPosition(binlogFile, binlogPosition);
    }

    public void registerSlave(String binlogFile, long binlogPosition, long slaveServerId) {
        RegisterSlaveHandler handler = new RegisterSlaveHandler(clientCapabilities, readPacketReader, writePacketWriter);
        this.masterServerId = handler.getServerId();
        logger.debug("serverId: {}", masterServerId);
        checksum = handler.handleChecksum();
        logger.debug("checksum: {}", checksum);
        handler.startBinlogDump(binlogFile, binlogPosition, slaveServerId);
    }

    public Either<ErrPacket, BinLogEvent> readBinlog() {
        if (binLogHandler == null) {
            binLogHandler = new BinLogHandler(readPacketReader, checksum);
        }
        return binLogHandler.readBinLogEvent();
    }

    public void close() {
        logger.debug("closing session");
        try {
            writePacketWriter.write(ComQuitPacket.INSTANCE);
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
        logger.debug("closed session");
    }

}
