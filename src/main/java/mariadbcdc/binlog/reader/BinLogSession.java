package mariadbcdc.binlog.reader;

import mariadbcdc.BinlogPosition;
import mariadbcdc.binlog.reader.handler.BinLogHandler;
import mariadbcdc.binlog.reader.handler.HandshakeHandler;
import mariadbcdc.binlog.reader.handler.HandshakeSuccessResult;
import mariadbcdc.binlog.reader.handler.RegisterSlaveHandler;
import mariadbcdc.binlog.reader.io.Either;
import mariadbcdc.binlog.reader.io.PacketIO;
import mariadbcdc.binlog.reader.packet.*;
import mariadbcdc.binlog.reader.packet.binlog.BinLogEvent;
import mariadbcdc.binlog.reader.packet.query.ComQueryPacket;
import mariadbcdc.binlog.reader.packet.result.ResultSetPacket;
import mariadbcdc.binlog.reader.packet.result.ResultSetRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.time.Duration;

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

    private long slaveServerId;
    private String readerId;

    public BinLogSession(ConnectionInfo connectionInfo, String readerId) {
        this.user = connectionInfo.getUser();
        this.password = connectionInfo.getPassword();
        this.readerId = readerId;
        try {
            socket = new Socket(connectionInfo.getHost(), connectionInfo.getPort());
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

    public void enableHeartbeat(Duration heartbeatPeriod) {
        writePacketWriter.write(new ComQueryPacket("set @master_heartbeat_period=" + (heartbeatPeriod.toNanos())));
        Either<OkPacket, ResultSetPacket> rs = readPacketReader.readResultSetPacket(this.clientCapabilities);
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
        logger.info("[readerId={}] fetch binlog filename/position: {}/{}", readerId, binlogFile, binlogPosition);
        return new BinlogPosition(binlogFile, binlogPosition);
    }

    public void registerSlave(String binlogFile, long binlogPosition, long slaveServerId) {
        RegisterSlaveHandler handler = new RegisterSlaveHandler(clientCapabilities, readPacketReader, writePacketWriter);
        this.masterServerId = handler.getServerId();
        logger.debug("[readerId={}] serverId: {}", readerId, masterServerId);
        checksum = handler.handleChecksum();
        logger.debug("[readerId={}]  checksum: {}", readerId, checksum);
        handler.startBinlogDump(binlogFile, binlogPosition, slaveServerId);
        this.slaveServerId = slaveServerId;
        logger.info("[readerId={}] [slaveServerId={}] started binlog dump", readerId, slaveServerId);
    }

    public Either<ErrPacket, BinLogEvent> readBinlog() {
        if (binLogHandler == null) {
            binLogHandler = new BinLogHandler(readPacketReader, checksum);
        }
        return binLogHandler.readBinLogEvent();
    }

    public void close() {
        logger.debug("[readerId={}] [slaveServerId={}] closing session", readerId, slaveServerId);
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
        logger.debug("[readerId={}] [slaveServerId={}] closed session", readerId, slaveServerId);
    }

}
