package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.event.*;
import mariadbcdc.BeforeAfterRow;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdateRowsEventBuilder {
    public static UpdateRowsEventBuilder withIncludedColumns(int[] includedColumnIdx) {
        UpdateRowsEventBuilder builder = new UpdateRowsEventBuilder();
        BitSet incCols = new BitSet();
        for (int columnIdx : includedColumnIdx) {
            incCols.set(columnIdx);
        }
        builder.incCols = incCols;
        return builder;
    }

    private BitSet incCols;
    private List<BeforeAfterRow> beforeAfterRows;
    private Long nextPosition;

    public UpdateRowsEventBuilder beforeAfterRows(List<BeforeAfterRow> rows) {
        this.beforeAfterRows = rows;
        return this;
    }

    public UpdateRowsEventBuilder nextPosition(Long nextPosition) {
        this.nextPosition = nextPosition;
        return this;
    }

    public Event build() {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.UPDATE_ROWS);
        header.setNextPosition(nextPosition);
        UpdateRowsEventData data = new UpdateRowsEventData();
        data.setIncludedColumnsBeforeUpdate(incCols);
        data.setIncludedColumns(incCols);
        List<Map.Entry<Serializable[], Serializable[]>> rows = beforeAfterRows.stream()
                .map(bar -> new AbstractMap.SimpleEntry<>(bar.getBefore(), bar.getAfter()))
                .collect(Collectors.toList());
        data.setRows(rows);
        return new Event(header, data);
    }
}
