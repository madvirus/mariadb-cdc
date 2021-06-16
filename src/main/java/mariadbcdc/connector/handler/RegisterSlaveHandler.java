package mariadbcdc.connector.handler;

import mariadbcdc.connector.io.Either;
import mariadbcdc.connector.packet.OkPacket;
import mariadbcdc.connector.packet.ReadPacketReader;
import mariadbcdc.connector.packet.WritePacketWriter;
import mariadbcdc.connector.packet.binlog.ComBinlogDumpPacket;
import mariadbcdc.connector.packet.query.ComQueryPacket;
import mariadbcdc.connector.packet.result.ResultSetPacket;
import mariadbcdc.connector.packet.result.ResultSetRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RegisterSlaveHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private int clientCapacilities;
    private ReadPacketReader reader;
    private WritePacketWriter writer;

    public RegisterSlaveHandler(int clientCapacilities,
                                ReadPacketReader reader,
                                WritePacketWriter writer) {
        this.clientCapacilities = clientCapacilities;
        this.reader = reader;
        this.writer = writer;
    }

    public Long getServerId() {
        writer.write(new ComQueryPacket("SHOW VARIABLES LIKE 'SERVER_ID'"));
        Either<OkPacket, ResultSetPacket> rs = reader.readResultSetPacket(clientCapacilities);
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
        Either<OkPacket, ResultSetPacket> rs = reader.readResultSetPacket(clientCapacilities);
        writer.write(new ComQueryPacket("SELECT @master_binlog_checksum"));
        Either<OkPacket, ResultSetPacket> rs2 = reader.readResultSetPacket(clientCapacilities);
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
