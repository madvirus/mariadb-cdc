package mariadbcdc;

import java.util.Collections;
import java.util.List;

@FunctionalInterface
public interface ColumnNamesGetter {
    ColumnNamesGetter NULL = new NullColumnNamesGetter();

    List<String> getColumnNames(String database, String table);

    class NullColumnNamesGetter implements ColumnNamesGetter {
        @Override
        public List<String> getColumnNames(String database, String table) {
            return Collections.emptyList();
        }
    }
}
