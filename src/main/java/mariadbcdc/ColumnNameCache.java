package mariadbcdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnNameCache {
    private Logger logger = LoggerFactory.getLogger(getClass());

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
        if (database != null && !database.isEmpty()) {
            String key = colNamesKey(database, table);
            colNames.remove(key);
            logger.info("invalidate column name cache: {}", key);
        } else {
            colNames.keySet().stream().filter(key -> key.endsWith("." + table))
                    .collect(Collectors.toList())
                    .forEach(key -> {
                        colNames.remove(key);
                        logger.info("invalidate column name cache: {}", key);
                    });
        }
    }

    private String colNamesKey(String database, String table) {
        return database + "." + table;
    }
}
