package mariadbcdc.binlog.reader.handler;

import mariadbcdc.binlog.reader.io.Either;
import mariadbcdc.binlog.reader.packet.OkPacket;
import mariadbcdc.binlog.reader.packet.ReadPacketReader;
import mariadbcdc.binlog.reader.packet.WritePacketWriter;
import mariadbcdc.binlog.reader.packet.binlog.ComBinlogDumpPacket;
import mariadbcdc.binlog.reader.packet.query.ComQueryPacket;
import mariadbcdc.binlog.reader.packet.result.ResultSetPacket;
import mariadbcdc.binlog.reader.packet.result.ResultSetRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RegisterSlaveHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private int clientCapabilities;
    private ReadPacketReader reader;
    private WritePacketWriter writer;

    public RegisterSlaveHandler(int clientCapabilities,
                                ReadPacketReader reader,
                                WritePacketWriter writer) {
        this.clientCapabilities = clientCapabilities;
        this.reader = reader;
        this.writer = writer;
    }

    public Long getServerId() {
        writer.write(new ComQueryPacket("SHOW VARIABLES LIKE 'SERVER_ID'"));
        Either<OkPacket, ResultSetPacket> rs = reader.readResultSetPacket(clientCapabilities);
        if (rs.isLeft()) {
            // OK
            return null;
        }
        List<ResultSetRow> rows = rs.getRight().getRows();
        if (!rows.isEmpty()) {
            return null;
        }
        return rows.get(0).getLong(1);
    }

    public String handleChecksum() {
        writer.write(new ComQueryPacket("SET @master_binlog_checksum= @@global.binlog_checksum"));
        Either<OkPacket, ResultSetPacket> rs = reader.readResultSetPacket(clientCapabilities);
        writer.write(new ComQueryPacket("SELECT @master_binlog_checksum"));
        Either<OkPacket, ResultSetPacket> rs2 = reader.readResultSetPacket(clientCapabilities);
        if (rs2.isLeft()) {
            return null;
        }
        return rs2.getRight().getRows().get(0).getString(0);
    }

    public void startBinlogDump(String binlogFilename, long binlogPosition, long slaveServerId) {
        ComBinlogDumpPacket dumpPacket = new ComBinlogDumpPacket(
                binlogPosition,
                0,
                slaveServerId,
                binlogFilename
        );
        writer.write(dumpPacket);
        logger.debug("start binlog dump");
    }
}
