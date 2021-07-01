package mariadbcdc.binlog.reader.packet.binlog.des;

import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.reader.packet.binlog.BinLogStatus;
import mariadbcdc.binlog.reader.packet.binlog.data.FormatDescriptionEvent;
import mariadbcdc.binlog.reader.packet.binlog.data.TableMapEvent;

import java.util.Map;

public class FormatDescriptionEventBinLogDataDeserializer implements BinLogDataDeserializer {

    @Override
    public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        int logFormatVersion = readPacketData.readInt(2);
        String serverVersion = readPacketData.readString(50).trim();
        long timestamp = readPacketData.readLong(4) * 1000;
        int headerLength = readPacketData.readInt(1);
        // n = event_size - header length(19) - offset (2 + 50 + 4 + 1) - checksum_algo - checksum
        int n = (int) header.getEventLength() - headerLength - (2 + 50 + 4 + 1) - 5;
        if (n > 0) {
            readPacketData.skip(n);
        }
        int checksumType = readPacketData.readInt(1);
        FormatDescriptionEvent event = new FormatDescriptionEvent(
                logFormatVersion,
                serverVersion,
                timestamp,
                headerLength,
                checksumType
        );
        return event;
    }
}
