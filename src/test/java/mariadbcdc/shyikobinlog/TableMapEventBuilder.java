package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

public class TableMapEventBuilder {
    private String database;
    private String table;
    private byte[] columnTypes;
    private Long nextPosition;

    public static TableMapEventBuilder withDatabase(String database, String table) {
        TableMapEventBuilder builder = new TableMapEventBuilder();
        builder.database = database;
        builder.table = table;
        return builder;
    }

    public TableMapEventBuilder columnTypes(ColumnType[] columnTypes) {
        this.columnTypes = new byte[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            this.columnTypes[i] = (byte) columnTypes[i].getCode();
        }
        return this;
    }

    public TableMapEventBuilder nextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
        return this;
    }

    public Event build() {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.TABLE_MAP);
        header.setNextPosition(nextPosition);
        TableMapEventData data = new TableMapEventData();
        data.setDatabase(database);
        data.setTable(table);
        data.setColumnTypes(columnTypes);
        return new Event(header, data);
    }
}
