package mariadbcdc.connector.handler;

import mariadbcdc.connector.binlog.ComBinlogDumpPacket;
import mariadbcdc.connector.io.Either;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.OkPacket;
import mariadbcdc.connector.packet.query.ComQueryPacket;
import mariadbcdc.connector.packet.result.ResultSetPacket;
import mariadbcdc.connector.packet.result.ResultSetRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RegisterSlaveHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private PacketIO packetIO;

    public RegisterSlaveHandler(PacketIO packetIO) {
        this.packetIO = packetIO;
    }

    public Long getServerId() {
        packetIO.write(new ComQueryPacket("SHOW VARIABLES LIKE 'SERVER_ID'"));
        Either<OkPacket, ResultSetPacket> rs = packetIO.readResultSetPacket();
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
        packetIO.write(new ComQueryPacket("SET @master_binlog_checksum= @@global.binlog_checksum"));
        Either<OkPacket, ResultSetPacket> rs = packetIO.readResultSetPacket();
        packetIO.write(new ComQueryPacket("SELECT @master_binlog_checksum"));
        Either<OkPacket, ResultSetPacket> rs2 = packetIO.readResultSetPacket();
        if (rs2.isLeft()) {
            return null;
        }
        return rs2.getRight().getRows().get(0).getString(0);
    }

    public void startBinlogDump(String binlogFilename, long binlogPosition, long slaveServerId) {
        packetIO.write(new ComBinlogDumpPacket(
                binlogPosition,
                0,
                slaveServerId,
                binlogFilename
        ));
        logger.info("send");
    }
}
