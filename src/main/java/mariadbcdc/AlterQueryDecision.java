package mariadbcdc;

public class AlterQueryDecision {
    private boolean alterQuery;
    private String database;
    private String table;

    public AlterQueryDecision(boolean alterQuery, String database, String table) {
        this.alterQuery = alterQuery;
        this.database = database;
        this.table = table;
    }

    public boolean isAlterQuery() {
        return alterQuery;
    }

    public boolean hasDatabase() {
        return database != null && !database.isEmpty();
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }
}
