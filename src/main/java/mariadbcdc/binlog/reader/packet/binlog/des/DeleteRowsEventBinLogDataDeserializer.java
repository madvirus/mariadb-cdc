package mariadbcdc.binlog.reader.packet.binlog.des;

import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.reader.packet.binlog.BinLogStatus;
import mariadbcdc.binlog.reader.packet.binlog.data.DeleteRowsEvent;
import mariadbcdc.binlog.reader.packet.binlog.data.TableMapEvent;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class DeleteRowsEventBinLogDataDeserializer extends BaseRowsEventBinLogDataDeserializer {
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
        return new DeleteRowsEvent(
                tableId,
                numberOfColumns,
                columnUsed,
                rows
        );
    }

}
