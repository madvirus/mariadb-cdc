package mariadbcdc;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

class BinaryLogEventProcessorTest {

    private StubMariadbCdcListener listener = new StubMariadbCdcListener();
    private CurrentBinlogFilenameGetter currentBinlogFilenameGetter = mock(CurrentBinlogFilenameGetter.class);
    private BinlogPositionSaver binlogPositionSaver = mock(BinlogPositionSaver.class);
    private ColumnNamesGetter columnNamesGetter = mock(ColumnNamesGetter.class);
    private StubSchemaChangeListener schemaChangeListener = new StubSchemaChangeListener();
    private BinaryLogEventProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new BinaryLogEventProcessor(
                listener,
                currentBinlogFilenameGetter,
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangeListener
        );
    }

    @Test
    void writeRows() {
        given(columnNamesGetter.getColumnNames("test", "member")).willReturn(Arrays.asList("ID", "NAME", "EMAIL"));

        processor.onEvent(memberTableMapEvent(1L));

        Event writeRowsEvent = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1, 2})
                .rows(Arrays.<Serializable[]>asList(
                        new Serializable[] {1L, "이름1", "이메일1"},
                        new Serializable[] {2L, "이름2", "이메일2"}
                ))
                .nextPosition(2L)
                .build();

        processor.onEvent(writeRowsEvent);

        List<RowChangedData> list = listener.getChangedDataList();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(list).hasSize(2);
            if (list.size() == 2) {
                soft.assertThat(list.get(0).getType()).isEqualTo(ChangeType.INSERT);
                soft.assertThat(list.get(0).getDatabase()).isEqualTo("test");
                soft.assertThat(list.get(0).getTable()).isEqualTo("member");
                soft.assertThat(list.get(0).getDataRow().getLong("id")).isEqualTo(1L);
                soft.assertThat(list.get(0).getDataRow().getString("name")).isEqualTo("이름1");
                soft.assertThat(list.get(0).getDataRow().getString("email")).isEqualTo("이메일1");
                soft.assertThat(list.get(1).getType()).isEqualTo(ChangeType.INSERT);
                soft.assertThat(list.get(1).getDatabase()).isEqualTo("test");
                soft.assertThat(list.get(1).getTable()).isEqualTo("member");
                soft.assertThat(list.get(1).getDataRow().getLong("id")).isEqualTo(2L);
                soft.assertThat(list.get(1).getDataRow().getString("name")).isEqualTo("이름2");
                soft.assertThat(list.get(1).getDataRow().getString("email")).isEqualTo("이메일2");
            }
        });
    }

    @Test
    void updateRows() {
        given(columnNamesGetter.getColumnNames("test", "member")).willReturn(Arrays.asList("ID", "NAME", "EMAIL"));

        processor.onEvent(memberTableMapEvent(1L));

        Event updateRowsEvent = UpdateRowsEventBuilder.withIncludedColumns(new int[] {0, 1, 2})
                .beforeAfterRows(Arrays.asList(
                        new BeforeAfterRow(new Serializable[] {1L, "이름", "이메일"}, new Serializable[] {1L, "이름1", "이메일1"})
                ))
                .nextPosition(2L)
                .build();

        processor.onEvent(updateRowsEvent);

        List<RowChangedData> list = listener.getChangedDataList();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(list).hasSize(1);
            if (list.size() == 1) {
                soft.assertThat(list.get(0).getType()).isEqualTo(ChangeType.UPDATE);
                soft.assertThat(list.get(0).getDatabase()).isEqualTo("test");
                soft.assertThat(list.get(0).getTable()).isEqualTo("member");
                soft.assertThat(list.get(0).getDataRow().getLong("id")).isEqualTo(1L);
                soft.assertThat(list.get(0).getDataRow().getString("name")).isEqualTo("이름1");
                soft.assertThat(list.get(0).getDataRow().getString("email")).isEqualTo("이메일1");
                soft.assertThat(list.get(0).getDataRowBeforeUpdate().getLong("id")).isEqualTo(1L);
                soft.assertThat(list.get(0).getDataRowBeforeUpdate().getString("name")).isEqualTo("이름");
                soft.assertThat(list.get(0).getDataRowBeforeUpdate().getString("email")).isEqualTo("이메일");
            }
        });
    }

    @Test
    void deleteRows() {
        given(columnNamesGetter.getColumnNames("test", "member")).willReturn(Arrays.asList("ID", "NAME", "EMAIL"));

        processor.onEvent(memberTableMapEvent(1L));

        Event deleteRowsEvent = DeleteRowsEventBuilder.withIncludedColumns(new int[] {0, 1, 2})
                .rows(Arrays.<Serializable[]>asList(
                        new Serializable[] {1L, "이름", "이메일"}
                ))
                .nextPosition(2L)
                .build();

        processor.onEvent(deleteRowsEvent);

        List<RowChangedData> list = listener.getChangedDataList();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(list).hasSize(1);
            if (list.size() == 1) {
                soft.assertThat(list.get(0).getType()).isEqualTo(ChangeType.DELETE);
                soft.assertThat(list.get(0).getDatabase()).isEqualTo("test");
                soft.assertThat(list.get(0).getTable()).isEqualTo("member");
                soft.assertThat(list.get(0).getDataRow().getLong("id")).isEqualTo(1L);
                soft.assertThat(list.get(0).getDataRow().getString("name")).isEqualTo("이름");
                soft.assertThat(list.get(0).getDataRow().getString("email")).isEqualTo("이메일");
            }
        });
    }

    private Event memberTableMapEvent(long nextPosition) {
        return TableMapEventBuilder.withDatabase("test", "member")
                .columnTypes(new ColumnType[]{ColumnType.LONG, ColumnType.VARCHAR, ColumnType.VARCHAR})
                .nextPosition(nextPosition)
                .build();
    }

    @Test
    void noPrececedTableMapEvent_Then_Skip_RowEvent() {
        Event writeRowsEvent = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1, 2})
                .rows(Arrays.<Serializable[]>asList(
                        new Serializable[] {1L, "이름1", "이메일1"},
                        new Serializable[] {2L, "이름2", "이메일2"}
                ))
                .nextPosition(2L)
                .build();

        processor.onEvent(writeRowsEvent);

        assertThat(listener.getChangedDataList()).isEmpty();
    }

    @Test
    void colNames_is_not_match_colTypes_Then_use_default_ColNames() {
        given(columnNamesGetter.getColumnNames("test", "member"))
                .willReturn(Arrays.asList("COLNAME1", "COLNAME2", "COLNAME3", "COLNAME4"));

        processor.onEvent(memberTableMapEvent(1L));

        Event writeRowsEvent = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1, 2})
                .rows(Arrays.<Serializable[]>asList(
                        new Serializable[] {1L, "이름1", "이메일1"},
                        new Serializable[] {2L, "이름2", "이메일2"}
                ))
                .nextPosition(2L)
                .build();

        processor.onEvent(writeRowsEvent);

        List<RowChangedData> list = listener.getChangedDataList();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(list).hasSize(2);
            if (list.size() == 2) {
                soft.assertThat(list.get(0).getType()).isEqualTo(ChangeType.INSERT);
                soft.assertThat(list.get(0).getDatabase()).isEqualTo("test");
                soft.assertThat(list.get(0).getTable()).isEqualTo("member");
                soft.assertThat(list.get(0).getDataRow().getLong("col1")).isEqualTo(1L);
                soft.assertThat(list.get(0).getDataRow().getString("col2")).isEqualTo("이름1");
                soft.assertThat(list.get(0).getDataRow().getString("col3")).isEqualTo("이메일1");
            }
        });
    }

    @Test
    void txid() {
        processor.onEvent(XidEventBuilder.xid(1L).nextPosition(1L).build());

        Long lastXid = listener.getLastXid();
        assertThat(lastXid).isEqualTo(1L);
    }

    @Test
    void tableMapEvent_no_save_position() {
        processor.onEvent(memberTableMapEvent(1L));

        then(binlogPositionSaver).should(never()).save(any());
    }

    @Test
    void formatDescriptionEvent_doesnt_save_binpos() {
        processor.onEvent(FormatDescriptionEventBuilder.nextPosition(0L).build());
        then(this.binlogPositionSaver).should(Mockito.never()).save(any());
    }

    @Test
    void save_Binpos_Although_Listener_throws_exception() {
        listener.setThrowException();

        processor.onEvent(memberTableMapEvent(1L));

        Event writeRowsEvent = WriteRowsEventBuilder.withIncludedColumns(new int[] {0, 1, 2})
                .rows(Arrays.<Serializable[]>asList(
                        new Serializable[] {1L, "이름1", "이메일1"},
                        new Serializable[] {2L, "이름2", "이메일2"}
                ))
                .nextPosition(2L)
                .build();

        processor.onEvent(writeRowsEvent);

        processor.onEvent(XidEventBuilder.xid(1L).nextPosition(1L).build());
    }

    @Test
    void schemaChanged() {
        processor.onEvent(QueryEventBuilder
                .withDatabase("test")
                .sql("# dum")
                .nextPosition(1L)
                .build());

        processor.onEvent(QueryEventBuilder
                .withDatabase("test")
                .sql("alter table mysys.user add column birthday date")
                .nextPosition(2L)
                .build());

        List<SchemaChangedData> list = schemaChangeListener.getSchemaChangedDataList();
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(list).hasSize(1);
            soft.assertThat(list.get(0).getDatabase()).isEqualTo("mysys");
            soft.assertThat(list.get(0).getTable()).isEqualTo("user");
        });
    }

    private class StubMariadbCdcListener implements MariadbCdcListener {
        private List<RowChangedData> rowChangedDataList = new ArrayList<>();
        private Long lastXid;
        private boolean throwException;

        @Override
        public void onDataChanged(List<RowChangedData> list) {
            if (throwException) throw new RuntimeException("!");
            rowChangedDataList.addAll(list);
        }

        public List<RowChangedData> getChangedDataList() {
            return rowChangedDataList;
        }

        @Override
        public void onXid(Long xid) {
            if (throwException) throw new RuntimeException("!");
            this.lastXid = xid;
        }

        public Long getLastXid() {
            return lastXid;
        }

        public void setThrowException() {
            this.throwException = true;
        }
    }

    private class StubSchemaChangeListener implements SchemaChangeListener {
        private List<SchemaChangedData> schemaChangedDataList = new ArrayList<>();

        @Override
        public void onSchemaChanged(SchemaChangedData schemaChangedData) {
            schemaChangedDataList.add(schemaChangedData);
        }

        public List<SchemaChangedData> getSchemaChangedDataList() {
            return schemaChangedDataList;
        }
    }
}