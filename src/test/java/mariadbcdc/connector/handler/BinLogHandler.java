package mariadbcdc.connector.handler;

import mariadbcdc.connector.io.DumpUtil;
import mariadbcdc.connector.io.Either;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.ErrPacket;
import mariadbcdc.connector.packet.binlog.*;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;
import mariadbcdc.connector.packet.binlog.des.BinLogDataDeserializer;
import mariadbcdc.connector.packet.binlog.des.BinLogDataDeserializers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BinLogHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final PacketIO packetIO;
    private final String checksum;

    private Map<Long, TableMapEvent> tableMap = new HashMap<>();

    public BinLogHandler(PacketIO packetIO, String checksum) {
        this.packetIO = packetIO;
        this.checksum = checksum;
    }

    public Either<ErrPacket, BinLogEvent> readBinLogEvent() {
        BinLogStatus binLogStatus = readBinLogStatus();
        int status = binLogStatus.getStatus();
        if (status == 0x00) { // OK
            BinLogEvent binLogEvent = readBinLogEvent(binLogStatus);
            if (binLogEvent != null && binLogEvent.getHeader() != null && binLogEvent.getHeader().getEventType() == BinlogEventType.TABLE_MAP_EVENT) {
                TableMapEvent tableMapEvent = (TableMapEvent) binLogEvent.getData();
                tableMap.put(tableMapEvent.getTableId(), tableMapEvent);
            }
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
        if (logger.isTraceEnabled()) {
            logger.trace("binLogStatus: {}", binLogStatus);
        }
        return binLogStatus;
    }

    private BinLogEvent readBinLogEvent(BinLogStatus binLogStatus) {
        BinLogHeader header = readBinLogHeader();
        BinLogData data = readBinLogData(binLogStatus, header);
        return new BinLogEvent(header, data);
    }

    private BinLogHeader readBinLogHeader() {
        long timestamp = packetIO.readLong(4) * 1000L;
        int code = packetIO.readInt(1);
        long serverId = packetIO.readLong(4);
        long eventLength = packetIO.readLong(4);
        long nextPosition = packetIO.readLong(4);
        int flags = packetIO.readInt(2);
        BinLogHeader header = new BinLogHeader(
                timestamp,
                code,
                serverId,
                eventLength,
                nextPosition,
                flags
        );
        if (logger.isTraceEnabled()) {
            logger.trace("binlog header: {}", header);
        }
        return header;
    }

    @Nullable
    private BinLogData readBinLogData(BinLogStatus binLogStatus, BinLogHeader header) {
        if (logger.isTraceEnabled()) {
             packetIO.startDump();
        }
        packetIO.startBlock((int) header.getEventDataLength() - checksumSize());
        BinLogDataDeserializer deserializer = BinLogDataDeserializers.getDeserializer(header.getEventType());
        BinLogData data = null;
        if (deserializer != null) {
            data = deserializer.deserialize(packetIO, binLogStatus, header, tableMap);
        } else {
            packetIO.skipRemaining();
        }
        packetIO.skip(checksumSize());
        if (logger.isTraceEnabled()) {
            byte[] bytes = packetIO.finishDump();
            StringBuilder sb = new StringBuilder();
            DumpUtil.dumpHex(sb, bytes, 0, bytes.length);
            logger.trace("bin log hex: \n{}", sb.toString());
        }
        if (logger.isTraceEnabled()) {
            logger.trace("binlog data: {}", data);
        }
        return data;
    }

    private int checksumSize() {
        return 4; // "CRC32".equalsIgnoreCase(checksum) ? 4 : 0;
    }

}
