package mariadbcdc.connector.packet.binlog.data;

import mariadbcdc.connector.packet.binlog.BinLogData;

import java.util.BitSet;
import java.util.List;

public class UpdateRowsEvent implements BinLogData {
    private final long tableId;
    private final int numberOfColumns;
    private final BitSet columnUsed;
    private final BitSet updateColumnUsed;
    private final List<RowsPair> pairs;

    public UpdateRowsEvent(long tableId,
                           int numberOfColumns,
                           BitSet columnUsed,
                           BitSet updateColumnUsed,
                           List<RowsPair> pairs) {
        this.tableId = tableId;
        this.numberOfColumns = numberOfColumns;
        this.columnUsed = columnUsed;
        this.updateColumnUsed = updateColumnUsed;
        this.pairs = pairs;
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

    public BitSet getUpdateColumnUsed() {
        return updateColumnUsed;
    }

    public List<RowsPair> getPairs() {
        return pairs;
    }
}
