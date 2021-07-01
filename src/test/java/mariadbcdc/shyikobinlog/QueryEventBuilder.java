package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.event.*;

public class QueryEventBuilder {
    private String database;
    private String sql;
    private Long nextPosition;

    public static QueryEventBuilder withDatabase(String database) {
        QueryEventBuilder builder = new QueryEventBuilder();
        builder.database = database;
        return builder;
    }

    public QueryEventBuilder sql(String sql) {
        this.sql = sql;
        return this;
    }

    public QueryEventBuilder nextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
        return this;
    }

    public Event build() {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.QUERY);
        header.setNextPosition(nextPosition);
        QueryEventData data = new QueryEventData();
        data.setDatabase(database);
        data.setSql(sql);
        return new Event(header, data);
    }
}
