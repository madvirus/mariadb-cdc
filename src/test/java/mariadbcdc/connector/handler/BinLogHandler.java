package mariadbcdc.connector.handler;

import mariadbcdc.connector.binlog.BinLogEvent;
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

    public Either<ErrPacket, BinLogEvent> readBinLog() {
        ReadPacketData readPacketData = packetIO.readPacketData();
        int status = readPacketData.readInt(1);
        CRC32 crc32 = new CRC32();
        crc32.update(readPacketData.getRawBodyBytes(), 0, readPacketData.getPacketLength() - 4);
        logger.info("crc32: {}", Long.toHexString(crc32.getValue()));

        if (status == 0x00) { // OK
            logger.info("OK");
            return null;
        } else if (status == 0xFF) { // ERR
            return Either.left(ErrPacket.from(readPacketData));
        } else if (status == 0xFE) { // EOF
            return Either.right(BinLogEvent.EOF);
        } else {
            return Either.right(BinLogEvent.UNKNOWN);
        }
    }
}
