package mariadbcdc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnNameCache {
    private Map<String, List<String>> colNames = new HashMap<>();

    private ColumnNamesGetter columnNamesGetter;

    public ColumnNameCache(ColumnNamesGetter columnNamesGetter) {
        this.columnNamesGetter = columnNamesGetter;
    }

    public List<String> getColumnNames(String database, String table) {
        String key = colNamesKey(database, table);
        List<String> cached = this.colNames.get(key);
        if (cached != null) return cached;
        List<String> names = columnNamesGetter.getColumnNames(database, table);
        colNames.put(key, names);
        return names;
    }

    public void invalidate(String database, String table) {
        colNames.remove(colNamesKey(database, table));
    }

    private String colNamesKey(String database, String table) {
        return database + "." + table;
    }
}
