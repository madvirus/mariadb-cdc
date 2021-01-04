package mariadbcdc.connector.handler;

import mariadbcdc.connector.binlog.BinLogEvent;
import mariadbcdc.connector.binlog.BinLogHeader;
import mariadbcdc.connector.binlog.BinlogEventType;
import mariadbcdc.connector.io.Either;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.ErrPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.zip.CRC32;

public class BinLogHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final PacketIO packetIO;
    private final String checksum;

    public BinLogHandler(PacketIO packetIO, String checksum) {
        this.packetIO = packetIO;
        this.checksum = checksum;
    }

    public Either<ErrPacket, BinLogEvent> readBinLogEvent() {
        ReadPacketData readPacketData = packetIO.readPacketData();
        int status = readPacketData.readInt(1);

        if (status == 0x00) { // OK
            logger.info("OK");
            BinLogEvent binLogEvent = toBinLogEvent(readPacketData);
            return binLogEvent == null ?
                    Either.right(BinLogEvent.EOF) :
                    Either.right(binLogEvent);
        } else if (status == 0xFF) { // ERR
            return Either.left(ErrPacket.from(readPacketData));
        } else if (status == 0xFE) { // EOF
            return Either.right(BinLogEvent.EOF);
        } else {
            return Either.right(BinLogEvent.UNKNOWN);
        }
    }

    private BinLogEvent toBinLogEvent(ReadPacketData readPacketData) {
        BinLogHeader header = readBinLogHeader(readPacketData);
        return null;
    }

    private BinLogHeader readBinLogHeader(ReadPacketData readPacketData) {
        return new BinLogHeader(
                readPacketData.readLong(4) * 1000L,
                toBinLogEventType(readPacketData.readInt(1)),
                readPacketData.readLong(4),
                readPacketData.readLong(4),
                readPacketData.readLong(4),
                readPacketData.readInt(2)
        );
    }

    private BinlogEventType toBinLogEventType(int code) {
        return BinlogEventType.byCode(code);
    }
}
