package mariadbcdc;

import java.util.Objects;

public class SchemaChangedTable {
    private String database;
    private String table;

    public SchemaChangedTable(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaChangedTable that = (SchemaChangedTable) o;
        return Objects.equals(database, that.database) &&
                Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }

    @Override
    public String toString() {
        return "SchemaChangedTable{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
