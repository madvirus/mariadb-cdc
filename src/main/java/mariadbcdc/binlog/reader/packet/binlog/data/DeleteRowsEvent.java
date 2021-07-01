package mariadbcdc.binlog.reader.packet.binlog.data;

import mariadbcdc.binlog.reader.packet.binlog.BinLogData;

import java.util.BitSet;
import java.util.List;

public class DeleteRowsEvent implements BinLogData {
    private final long tableId;
    private final int numberOfColumns;
    private final BitSet columnUsed;
    private final List<Object[]> rows;

    public DeleteRowsEvent(long tableId,
                           int numberOfColumns,
                           BitSet columnUsed,
                           List<Object[]> rows) {
        this.tableId = tableId;
        this.numberOfColumns = numberOfColumns;
        this.columnUsed = columnUsed;
        this.rows = rows;
    }

    public long getTableId() {
        return tableId;
    }

    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    public BitSet getColumnUsed() {
        return columnUsed;
    }

    public List<Object[]> getRows() {
        return rows;
    }
}
