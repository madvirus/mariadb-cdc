package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;
import mariadbcdc.connector.packet.binlog.data.WriteRowsEvent;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class WriteRowsEventBinLogDataDeserializer extends BaseRowsEventBinLogDataDeserializer {
    @Override
    public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        long tableId = readPacketData.readLong(6);
        int flags = readPacketData.readInt(2);
        int numberOfColumns = readPacketData.readLengthEncodedInt();
        byte[] bitMap = new byte[(numberOfColumns + 7) / 8];
        readPacketData.readBytes(bitMap);
        BitSet columnUsed = BitSet.valueOf(bitMap);
        List<Object[]> rows = new ArrayList<>();

        while(readPacketData.remaining() > 0) {
            readPacketData.readBytes(bitMap);
            BitSet nullBitmap = BitSet.valueOf(bitMap);
            Object[] columnValues = readColumnValues(columnUsed, nullBitmap, tableMap.get(tableId), readPacketData);
            rows.add(columnValues);
        }
        return new WriteRowsEvent(
                tableId,
                numberOfColumns,
                columnUsed,
                rows
        );
    }

}
