package mariadbcdc.binlog;

import mariadbcdc.binlog.reader.FieldType;

import java.util.List;

public class TableInfo {
    private long tableId;
    private String database;
    private String table;
    private FieldType[] columnTypes;
    private List<String> columnNamesOfMetadata;

    public TableInfo(long tableId, String database, String table, FieldType[] columnTypes, List<String> columnNamesOfMetadata) {
        this.tableId = tableId;
        this.database = database;
        this.table = table;
        this.columnTypes = columnTypes;
        this.columnNamesOfMetadata = columnNamesOfMetadata;
    }

    public long getTableId() {
        return tableId;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public FieldType[] getColumnTypes() {
        return columnTypes;
    }

    public List<String> getColumnNamesOfMetadata() {
        return columnNamesOfMetadata;
    }

    public boolean hasDatabaseTableName() {
        return database != null && database.length() > 0 && table != null && table.length() > 0;
    }
}
