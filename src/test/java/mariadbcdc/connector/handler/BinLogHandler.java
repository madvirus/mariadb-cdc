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
                break;
            case TABLE_MAP_EVENT:

            default:
                packetIO.skipRemaining();
        }
        packetIO.skip(checksumSize());
        return new BinLogEvent(header, data);
    }

    private int checksumSize() {
        return 4; // "CRC32".equalsIgnoreCase(checksum) ? 4 : 0;
    }

    private BinLogHeader readBinLogHeader() {
        long timestamp = packetIO.readLong(4) * 1000L;
        int code = packetIO.readInt(1);
        long serverId = packetIO.readLong(4);
        long eventLength = packetIO.readLong(4);
        long nextPosition = packetIO.readLong(4);
        int flags = packetIO.readInt(2);
        return new BinLogHeader(
                timestamp,
                code,
                serverId,
                eventLength,
                nextPosition,
                flags
        );
    }

    private BinLogData readRotateEvent(BinLogStatus binLogStatus, BinLogHeader header) {
        RotateEvent rotateEvent = new RotateEvent(packetIO.readLong(8), packetIO.readStringEOF());
        logger.info("read RotateEvent: {}", rotateEvent);
        return rotateEvent;
    }

    private BinLogData readFormationDescriptionEvent(BinLogStatus binLogStatus, BinLogHeader header) {
        int logFormatVersion = packetIO.readInt(2);
        String serverVersion = packetIO.readString(50).trim();
        long timestamp = packetIO.readLong(4) * 1000;
        int headerLength = packetIO.readInt(1);
        // n = event_size - header length(19) - offset (2 + 50 + 4 + 1) - checksum_algo - checksum
        int n = (int)header.getEventLength() - headerLength - (2 + 50 + 4 + 1) - 5;
        if (n > 0) {
            packetIO.skip(n);
        }
        int checksumType = packetIO.readInt(1);
        FormatDescriptionEvent event = new FormatDescriptionEvent(
                logFormatVersion,
                serverVersion,
                timestamp,
                headerLength,
                checksumType
        );
        logger.info("read FormatDescriptionEvent: {}", event);
        return event;
    }

}
