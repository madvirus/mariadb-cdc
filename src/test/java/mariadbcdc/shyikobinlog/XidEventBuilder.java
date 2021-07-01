package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.XidEventData;

public class XidEventBuilder {
    private Long nextPosition;
    private long xid;

    public static XidEventBuilder xid(long i) {
        XidEventBuilder builder = new XidEventBuilder();
        builder.xid = i;
        return builder;
    }

    public XidEventBuilder nextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
        return this;
    }

    public Event build() {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.XID);
        header.setNextPosition(nextPosition);
        XidEventData data = new XidEventData();
        data.setXid(xid);
        return new Event(header, data);
    }
}
