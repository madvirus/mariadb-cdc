package mariadbcdc;

public class SchemaChangedData {
    private String database;
    private String table;

    public SchemaChangedData(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }
}
