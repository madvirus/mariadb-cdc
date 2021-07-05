package mariadbcdc.binlog.reader.packet.binlog.data;

import java.util.List;

public class FullMeta {
    private List<String> columnNames;

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }
}
