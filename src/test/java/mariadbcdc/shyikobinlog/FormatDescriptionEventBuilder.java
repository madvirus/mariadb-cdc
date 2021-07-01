package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;

public class FormatDescriptionEventBuilder {
    private Long nextPosition;

    public static FormatDescriptionEventBuilder nextPosition(Long nextPosition) {
        FormatDescriptionEventBuilder builder = new FormatDescriptionEventBuilder();
        builder.nextPosition = nextPosition;
        return builder;
    }

    public Event build() {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.FORMAT_DESCRIPTION);
        header.setNextPosition(nextPosition);
        FormatDescriptionEventData data = new FormatDescriptionEventData();
        data.setBinlogVersion(4);
        data.setServerVersion("10.4.14-MariaDB-log");
        data.setHeaderLength(19);
        data.setDataLength(228);
        data.setChecksumType(ChecksumType.CRC32);
        return new Event(header, data);
    }
}
