package mariadbcdc.binlog.reader.packet.binlog.data;

import java.util.List;

public class FullMeta {
    private List<String> columnNames;
    private List<String> enumValues;

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }
}
