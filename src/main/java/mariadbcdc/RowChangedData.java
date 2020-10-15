package mariadbcdc;

public class RowChangedData {
    private ChangeType type;
    private String database;
    private String table;
    private long timestamp;
    private DataRow dataRow;
    private DataRow dataRowBeforeUpdate;

    public RowChangedData(ChangeType type, String database, String table, long timestamp, DataRow dataRow) {
        this.type = type;
        this.database = database;
        this.table = table;
        this.timestamp = timestamp;
        this.dataRow = dataRow;
    }

    public RowChangedData(ChangeType type, String database, String table, long timestamp, DataRow dataRow, DataRow dataRowBeforeUpdate) {
        this.type = type;
        this.database = database;
        this.table = table;
        this.timestamp = timestamp;
        this.dataRow = dataRow;
        this.dataRowBeforeUpdate = dataRowBeforeUpdate;
    }

    public ChangeType getType() {
        return type;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public DataRow getDataRow() {
        return dataRow;
    }

    public DataRow getDataRowBeforeUpdate() {
        return dataRowBeforeUpdate;
    }

}
