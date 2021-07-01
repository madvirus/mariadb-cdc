package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import mariadbcdc.*;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;

class BinaryLogEventProcessor_Filter_Test {

    private StubMariadbCdcListener listener = new StubMariadbCdcListener();
    private CurrentBinlogFilenameGetter currentBinlogFilenameGetter = mock(CurrentBinlogFilenameGetter.class);
    private BinlogPositionSaver binlogPositionSaver = mock(BinlogPositionSaver.class);
    private ColumnNamesGetter columnNamesGetter = mock(ColumnNamesGetter.class);

    @Test
    void noFilter() {
        BinaryLogEventProcessor processor= new BinaryLogEventProcessor(
                listener,
                currentBinlogFilenameGetter,
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangedData -> {});

        processor.onEvent(tableMapEvent("test", "member", 1L));
        Event writeRowsEvent = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1})
                .rows(Arrays.<Serializable[]>asList( new Serializable[] {"이름1", "이메일1"} ))
                .nextPosition(2L)
                .build();
        processor.onEvent(writeRowsEvent);

        processor.onEvent(tableMapEvent("test2", "user", 3L));
        Event updateRowsEvent = UpdateRowsEventBuilder.withIncludedColumns(new int[] {0, 1})
                .beforeAfterRows(Arrays.asList(new BeforeAfterRow(new Serializable[] {"사용자1", "이름"}, new Serializable[] {"사용자1", "이름1"})))
                .nextPosition(4L)
                .build();

        processor.onEvent(updateRowsEvent);

        List<RowChangedData> list = listener.getChangedDataList();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(list).hasSize(2);
            if (list.size() == 2) {
                soft.assertThat(list.get(0).getDatabase()).isEqualTo("test");
                soft.assertThat(list.get(0).getTable()).isEqualTo("member");
                soft.assertThat(list.get(1).getDatabase()).isEqualTo("test2");
                soft.assertThat(list.get(1).getTable()).isEqualTo("user");
            }
        });
    }

    @Test
    void excludeFilter() {
        BinaryLogEventProcessor processor= new BinaryLogEventProcessor(
                listener,
                currentBinlogFilenameGetter,
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangedData -> {});
        processor.setExcludeFilters("test.member", "test2.user");

        processor.onEvent(tableMapEvent("test", "member", 1L));
        Event writeRowsEvent = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1})
                .rows(Arrays.<Serializable[]>asList( new Serializable[] {"이름1", "이메일1"} ))
                .nextPosition(2L)
                .build();
        processor.onEvent(writeRowsEvent);

        processor.onEvent(tableMapEvent("test1", "member1", 3L));
        Event writeRowsEvent2 = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1})
                .rows(Arrays.<Serializable[]>asList( new Serializable[] {"이름1", "이메일1"} ))
                .nextPosition(4L)
                .build();
        processor.onEvent(writeRowsEvent2);

        processor.onEvent(tableMapEvent("test2", "user", 5L));
        Event updateRowsEvent = UpdateRowsEventBuilder.withIncludedColumns(new int[] {0, 1})
                .beforeAfterRows(Arrays.asList(new BeforeAfterRow(new Serializable[] {"사용자1", "이름"}, new Serializable[] {"사용자1", "이름1"})))
                .nextPosition(6L)
                .build();
        processor.onEvent(updateRowsEvent);

        List<RowChangedData> list = listener.getChangedDataList();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(list).hasSize(1);
            if (list.size() == 1) {
                soft.assertThat(list.get(0).getDatabase()).isEqualTo("test1");
                soft.assertThat(list.get(0).getTable()).isEqualTo("member1");
            }
        });
    }

    @Test
    void includeFilter() {
        BinaryLogEventProcessor processor= new BinaryLogEventProcessor(
                listener,
                currentBinlogFilenameGetter,
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangedData -> {});
        processor.setIncludeFilters("test.member", "test2.user");

        processor.onEvent(tableMapEvent("test", "member", 1L));
        Event writeRowsEvent = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1})
                .rows(Arrays.<Serializable[]>asList( new Serializable[] {"이름1", "이메일1"} ))
                .nextPosition(2L)
                .build();
        processor.onEvent(writeRowsEvent);

        processor.onEvent(tableMapEvent("test1", "member1", 3L));
        Event writeRowsEvent2 = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1})
                .rows(Arrays.<Serializable[]>asList( new Serializable[] {"이름1", "이메일1"} ))
                .nextPosition(4L)
                .build();
        processor.onEvent(writeRowsEvent2);

        processor.onEvent(tableMapEvent("test2", "user", 5L));
        Event updateRowsEvent = UpdateRowsEventBuilder.withIncludedColumns(new int[] {0, 1})
                .beforeAfterRows(Arrays.asList(new BeforeAfterRow(new Serializable[] {"사용자1", "이름"}, new Serializable[] {"사용자1", "이름1"})))
                .nextPosition(6L)
                .build();
        processor.onEvent(updateRowsEvent);

        List<RowChangedData> list = listener.getChangedDataList();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(list).hasSize(2);
            if (list.size() == 2) {
                soft.assertThat(list.get(0).getDatabase()).isEqualTo("test");
                soft.assertThat(list.get(0).getTable()).isEqualTo("member");
                soft.assertThat(list.get(1).getDatabase()).isEqualTo("test2");
                soft.assertThat(list.get(1).getTable()).isEqualTo("user");
            }
        });
    }

    private Event tableMapEvent(String database, String table, long nextPosition) {
        return TableMapEventBuilder.withDatabase(database, table)
                .columnTypes(new ColumnType[]{ColumnType.VARCHAR, ColumnType.VARCHAR})
                .nextPosition(nextPosition)
                .build();
    }


    private class StubMariadbCdcListener implements MariadbCdcListener {
        private List<RowChangedData> rowChangedDataList = new ArrayList<>();

        @Override
        public void onDataChanged(List<RowChangedData> list) {
            rowChangedDataList.addAll(list);
        }

        public List<RowChangedData> getChangedDataList() {
            return rowChangedDataList;
        }
    }
}