package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

public class DeleteRowsEventBuilder {
    public static DeleteRowsEventBuilder withIncludedColumns(int[] includedColumnIdx) {
        DeleteRowsEventBuilder builder = new DeleteRowsEventBuilder();
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

    public DeleteRowsEventBuilder rows(List<Serializable[]> rows) {
        this.rows = rows;
        return this;
    }

    public DeleteRowsEventBuilder nextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
        return this;
    }

    public Event build() {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.DELETE_ROWS);
        header.setNextPosition(nextPosition);
        DeleteRowsEventData data = new DeleteRowsEventData();
        data.setIncludedColumns(incCols);
        data.setRows(rows);
        return new Event(header, data);
    }
}
