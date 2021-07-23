package mariadbcdc.binlog;

import java.util.HashMap;
import java.util.Map;

public class TableInfos {
    private Map<Long, TableInfo> map = new HashMap<>();

    public void add(TableInfo tableInfo) {
        map.put(tableInfo.getTableId(), tableInfo);
    }

    public boolean hasPrecededDatabaseTableName(long tableId) {
        return map.containsKey(tableId);
    }

    public TableInfo getTableInfo(long tableId) {
        return map.get(tableId);
    }

    public void clear() {
        map.clear();
    }
}
