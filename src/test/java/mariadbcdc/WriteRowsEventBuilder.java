package mariadbcdc;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class WriteRowsEventBuilder {
    public static WriteRowsEventBuilder withIncludedColumns(int[] includedColumnIdx) {
        WriteRowsEventBuilder builder = new WriteRowsEventBuilder();
        BitSet incCols = new BitSet();
        for (int columnIdx : includedColumnIdx) {
            incCols.set(columnIdx);
        }
        builder.incCols = incCols;
        return builder;
    }


    private BitSet incCols;
    private List<Serializable[]> rows;
    private Long nextPosition;

    public WriteRowsEventBuilder rows(List<Serializable[]> rows) {
        this.rows = rows;
        return this;
    }

    public WriteRowsEventBuilder nextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
        return this;
    }


    public Event build() {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.WRITE_ROWS);
        header.setNextPosition(nextPosition);
        WriteRowsEventData data = new WriteRowsEventData();
        data.setIncludedColumns(incCols);
        data.setRows(rows);
        return new Event(header, data);
    }
}
