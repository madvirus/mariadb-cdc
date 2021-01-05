package mariadbcdc.connector.handler;

import mariadbcdc.connector.binlog.*;
import mariadbcdc.connector.io.Either;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.ErrPacket;
import mariadbcdc.connector.packet.binlog.BinLogStatus;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinLogHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final PacketIO packetIO;
    private final String checksum;

    public BinLogHandler(PacketIO packetIO, String checksum) {
        this.packetIO = packetIO;
        this.checksum = checksum;
    }

    public Either<ErrPacket, BinLogEvent> readBinLogEvent() {
        BinLogStatus binLogStatus = readBinLogStatus();
        logger.info("binLogStatus: {}", binLogStatus);
        int status = binLogStatus.getStatus();
        if (status == 0x00) { // OK
            BinLogEvent binLogEvent = readBinLogEvent(binLogStatus);
            return binLogEvent == null ?
                    Either.right(BinLogEvent.EOF) :
                    Either.right(binLogEvent);
        } else if (status == 0xFF) { // ERR
            return Either.left(ErrPacket.from(binLogStatus, packetIO));
        } else if (status == 0xFE) { // EOF
            return Either.right(BinLogEvent.EOF);
        } else {
            return Either.right(BinLogEvent.UNKNOWN);
        }
    }

    @NotNull
    private BinLogStatus readBinLogStatus() {
        BinLogStatus binLogStatus = new BinLogStatus(
                packetIO.readInt(3), packetIO.readInt(1), packetIO.readInt(1)
        );
        return binLogStatus;
    }

    private BinLogEvent readBinLogEvent(BinLogStatus binLogStatus) {
        BinLogHeader header = readBinLogHeader();
        logger.info("binlog header: {}", header);
        BinLogData data = null;
        packetIO.startBlock((int) header.getEventDataLength() - checksumSize());
        switch (header.getEventType()) {
            case ROTATE_EVENT:
                data = readRotateEvent(binLogStatus, header);
                break;
            case FORMAT_DESCRIPTION_EVENT:
                data = readFormationDescriptionEvent(binLogStatus, header);
            case TABLE_MAP_EVENT:

            default:
                packetIO.skipRemaining();
        }
        packetIO.skip(checksumSize());
        return new BinLogEvent(header, data);
    }

    private int checksumSize() {
        return "CRC32".equalsIgnoreCase(checksum) ? 4 : 0;
    }

    private BinLogHeader readBinLogHeader() {
        return new BinLogHeader(
                packetIO.readLong(4) * 1000L,
                toBinLogEventType(packetIO.readInt(1)),
                packetIO.readLong(4),
                packetIO.readLong(4),
                packetIO.readLong(4),
                packetIO.readInt(2)
        );
    }

    private BinLogData readRotateEvent(BinLogStatus binLogStatus, BinLogHeader header) {
        RotateEvent rotateEvent = new RotateEvent(packetIO.readLong(8), packetIO.readStringEOF());
        logger.info("read RotateEvent: {}", rotateEvent);
        return rotateEvent;
    }

    private BinLogData readFormationDescriptionEvent(BinLogStatus binLogStatus, BinLogHeader header) {
        FormatDescriptionEvent event = new FormatDescriptionEvent();
        return event;
    }

    private BinlogEventType toBinLogEventType(int code) {
        return BinlogEventType.byCode(code);
    }
}
