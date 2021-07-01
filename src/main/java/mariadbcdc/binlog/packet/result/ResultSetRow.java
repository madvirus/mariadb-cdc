package mariadbcdc.binlog.packet.result;

import mariadbcdc.binlog.io.ColumnDefPacket;

import java.util.Map;

public class ResultSetRow {

    private final Map<String, Integer> nameToIdx;
    private final ColumnDefPacket[] defs;
    private final TextResultSetRowPacket row;

    public ResultSetRow(Map<String, Integer> nameToIdx, ColumnDefPacket[] defs, TextResultSetRowPacket row) {
        this.nameToIdx = nameToIdx;
        this.defs = defs;
        this.row = row;
    }

    public String getString(int idx) {
        return row.getValues()[idx];
    }

    public Long getLong(int idx) {
        String str = row.getValues()[idx];
        if (str == null) return null;
        return Long.parseLong(str);
    }
}
