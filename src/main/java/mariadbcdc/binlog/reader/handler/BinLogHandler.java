package mariadbcdc.binlog.reader.handler;

import mariadbcdc.binlog.reader.io.Either;
import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.ErrPacket;
import mariadbcdc.binlog.reader.packet.ReadPacketReader;
import mariadbcdc.binlog.reader.packet.binlog.*;
import mariadbcdc.binlog.reader.packet.binlog.data.TableMapEvent;
import mariadbcdc.binlog.reader.packet.binlog.des.BinLogDataDeserializer;
import mariadbcdc.binlog.reader.packet.binlog.des.BinLogDataDeserializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BinLogHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private ReadPacketReader readPacketReader;
    private final String checksum;

    private Map<Long, TableMapEvent> tableMap = new HashMap<>();

    public BinLogHandler(ReadPacketReader readPacketReader, String checksum) {
        this.readPacketReader = readPacketReader;
        this.checksum = checksum;
    }

    public Either<ErrPacket, BinLogEvent> readBinLogEvent() {
        ReadPacketData readPacketData = readPacketReader.readPacketData();
        BinLogStatus binLogStatus = getBinLogStatus(readPacketData);
        int status = binLogStatus.getStatus();
        if (status == 0x00) { // OK
            BinLogEvent binLogEvent = readBinLogEvent(binLogStatus, readPacketData);
            if (binLogEvent != null && binLogEvent.getHeader() != null && binLogEvent.getHeader().getEventType() == BinlogEventType.TABLE_MAP_EVENT) {
                TableMapEvent tableMapEvent = (TableMapEvent) binLogEvent.getData();
                tableMap.put(tableMapEvent.getTableId(), tableMapEvent);
            }
            return binLogEvent == null ?
                    Either.right(BinLogEvent.EOF) :
                    Either.right(binLogEvent);
        } else if (status == 0xFF) { // ERR
            return Either.left(ErrPacket.from(binLogStatus, readPacketData));
        } else if (status == 0xFE) { // EOF
            return Either.right(BinLogEvent.EOF);
        } else {
            return Either.right(BinLogEvent.UNKNOWN);
        }
    }

    private BinLogStatus getBinLogStatus(ReadPacketData readPacketData) {
        BinLogStatus binLogStatus = new BinLogStatus(readPacketData.getPacketLength(), readPacketData.getSequenceNumber(),
                readPacketData.readInt(1));
        if (logger.isTraceEnabled()) {
            logger.trace("binLogStatus: {}", binLogStatus);
        }
        return binLogStatus;
    }

    private BinLogEvent readBinLogEvent(BinLogStatus binLogStatus, ReadPacketData readPacketData) {
        BinLogHeader header = readBinLogHeader(readPacketData);
        BinLogData data = readBinLogData(binLogStatus, header, readPacketData);
        return new BinLogEvent(header, data);
    }

    private BinLogHeader readBinLogHeader(ReadPacketData readPacketData) {
        long timestamp = readPacketData.readLong(4) * 1000L;
        int code = readPacketData.readInt(1);
        long serverId = readPacketData.readLong(4);
        long eventLength = readPacketData.readLong(4);
        long nextPosition = readPacketData.readLong(4);
        int flags = readPacketData.readInt(2);
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

    private BinLogData readBinLogData(BinLogStatus binLogStatus, BinLogHeader header, ReadPacketData readPacketData) {
        readPacketData.endBlock(readPacketData.getPacketLength() - checksumSize());
        BinLogDataDeserializer deserializer = BinLogDataDeserializers.getDeserializer(header.getEventType());
        return deserializer != null ?
            deserializer.deserialize(readPacketData, binLogStatus, header, tableMap) :
                null;
    }

    private int checksumSize() {
        return 4; // "CRC32".equalsIgnoreCase(checksum) ? 4 : 0;
    }

}
