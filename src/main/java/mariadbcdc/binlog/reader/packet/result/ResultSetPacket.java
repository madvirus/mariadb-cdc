package mariadbcdc.binlog.reader.packet.result;

import mariadbcdc.binlog.reader.io.ColumnDefPacket;
import mariadbcdc.binlog.reader.packet.ReadPacket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultSetPacket implements ReadPacket {
    private final ColumnCountPacket columnCountPacket;
    private final ColumnDefPacket[] columnDefPackets;
    private final List<TextResultSetRowPacket> rows;

    public ResultSetPacket(ColumnCountPacket columnCountPacket, ColumnDefPacket[] columnDefPackets, List<TextResultSetRowPacket> rows) {
        this.columnCountPacket = columnCountPacket;
        this.columnDefPackets = columnDefPackets;
        this.rows = rows;
    }

    public List<ResultSetRow> getRows() {
        List<ResultSetRow> ret = new ArrayList<>(rows.size());
        Map<String, Integer> nameToIdx = new HashMap<>();
        for (int i = 0 ; i < columnDefPackets.length ; i++) {
            nameToIdx.put(columnDefPackets[i].getColumn().toLowerCase(), i);
            nameToIdx.put(columnDefPackets[i].getColumnAlias().toLowerCase(), i);
        }
        for (int i = 0 ; i < rows.size() ; i++) {
            ResultSetRow r = new ResultSetRow(
                    nameToIdx,
                    columnDefPackets,
                    rows.get(i));
            ret.add(r);
        }
        return ret;
    }
}
