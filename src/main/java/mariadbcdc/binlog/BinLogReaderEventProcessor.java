package mariadbcdc.binlog;

import mariadbcdc.*;
import mariadbcdc.binlog.reader.BinLogListener;
import mariadbcdc.binlog.reader.FieldType;
import mariadbcdc.binlog.reader.packet.ErrPacket;
import mariadbcdc.binlog.reader.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.reader.packet.binlog.data.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BinLogReaderEventProcessor implements BinLogListener {
    private MariadbCdcListener listener;
    private CurrentBinlogFilenameGetter currentBinlogFilenameGetter;
    private BinlogPositionSaver binlogPositionSaver;
    private ColumnNamesGetter columnNamesGetter;
    private SchemaChangeListener schemaChangeListener;

    private Map<String, Boolean> includeFilters = Collections.emptyMap();
    private Map<String, Boolean> excludeFilters = Collections.emptyMap();

    private String currentBinlogFilename;

    private TableInfos tableInfos = new TableInfos();

    public BinLogReaderEventProcessor(MariadbCdcListener listener,
                                      CurrentBinlogFilenameGetter currentBinlogFilenameGetter,
                                      BinlogPositionSaver binlogPositionSaver,
                                      ColumnNamesGetter columnNamesGetter,
                                      SchemaChangeListener schemaChangeListener) {
        this.listener = listener;
        this.currentBinlogFilenameGetter = currentBinlogFilenameGetter;
        this.binlogPositionSaver = binlogPositionSaver;
        this.columnNamesGetter = columnNamesGetter;
        this.schemaChangeListener = schemaChangeListener;
    }

    @Override
    public void onErr(ErrPacket err) {
        BinLogListener.super.onErr(err);
    }

    @Override
    public void onRotateEvent(BinLogHeader header, RotateEvent data) {
        currentBinlogFilename = data.getFilename();
        binlogPositionSaver.save(new BinlogPosition(data.getFilename(), data.getPosition()));
    }

    @Override
    public void onFormatDescriptionEvent(BinLogHeader header, FormatDescriptionEvent data) {
        // do nothing
    }

    @Override
    public void onQueryEvent(BinLogHeader header, QueryEvent data) {
        SchemaChangeQueryDecision decision = QueryDecider.decideSchemaChangeQuery(data.getSql());
        if (decision.isAlterQuery()) {
            decision.getDatabaseTableNames()
                    .forEach(schemaChangedTable -> {
                        schemaChangeListener.onSchemaChanged(schemaChangedTable);
                    });
        }
    }

    @Override
    public void onTableMapEvent(BinLogHeader header, TableMapEvent tableData) {
        TableInfo tableInfo = new TableInfo(
                tableData.getTableId(),
                tableData.getDatabaseName(),
                tableData.getTableName(),
                tableData.getFieldTypes(),
                tableData.getFullMeta()
                        .map(fm -> fm.getColumnNames())
                        .filter(names -> names != null && !names.isEmpty())
                        .orElse(null)
        );
        tableInfos.add(tableInfo);
    }

    private List<String> includedColumnNames(List<String> colNames, BitSet includedColumns) {
        if (colNames == null || colNames.isEmpty()) {
            return Collections.emptyList();
        }
        return IntStream.range(0, colNames.size()).filter(i -> includedColumns.get(i))
                .mapToObj(i -> colNames.get(i))
                .collect(Collectors.toList());
    }

    private List<FieldType> includedColumnTypes(FieldType[] fieldTypes, BitSet includedColumns) {
        return IntStream.range(0, fieldTypes.length).filter(i -> includedColumns.get(i))
                .mapToObj(i -> fieldTypes[i])
                .collect(Collectors.toList());
    }

    @Override
    public void onWriteRowsEvent(BinLogHeader header, WriteRowsEvent data) {
        handleRowsEvent(header, data, new RowchangeDataFactory() {
            @Override
            public List<RowChangedData> create(BinlogPosition binlogPosition, TableInfo tableInfo, List<String> colNames) {
                List<String> incColNames = includedColumnNames(colNames, data.getColumnUsed());
                List<FieldType> incColTypes = includedColumnTypes(tableInfo.getColumnTypes(), data.getColumnUsed());

                List<RowChangedData> rowChangedDataList = data.getRows().stream()
                        .map(row -> new RowChangedData(
                                ChangeType.INSERT,
                                tableInfo.getDatabase(),
                                tableInfo.getTable(),
                                header.getTimestamp(),
                                convertDataRow(incColNames, incColTypes, row),
                                binlogPosition
                        ))
                        .collect(Collectors.toList());
                return rowChangedDataList;
            }
        });
    }

    interface RowchangeDataFactory {
        List<RowChangedData> create(BinlogPosition binLogPosition, TableInfo tableInfo, List<String> columnNames);
    }

    private <T extends RowsEvent> void handleRowsEvent(BinLogHeader header, T data,
                                                       RowchangeDataFactory factory) {
        final BinlogPosition currentEventBinLogPosition =
                new BinlogPosition(currentBinlogFilename, header.getNextPosition());
        try {
            TableInfo tableInfo = tableInfos.getTableInfo(data.getTableId());
            if (tableInfo != null && tableInfo.hasDatabaseTableName() && rowsEventDataIncluded(tableInfo)) {
                List<String> colNames = tableInfo.getColumnNamesOfMetadata() != null ? tableInfo.getColumnNamesOfMetadata() :
                        tableInfo.hasDatabaseTableName() ?
                                columnNamesGetter.getColumnNames(tableInfo.getDatabase(), tableInfo.getTable()) :
                                Collections.emptyList();

                if (colNames.size() > 0 && colNames.size() != tableInfo.getColumnTypes().length) {
                    colNames = Collections.emptyList();
                }
                List<RowChangedData> rowChangedDataList = factory.create(currentEventBinLogPosition, tableInfo, colNames);
                listener.onDataChanged(rowChangedDataList);
            }
        } finally {
            binlogPositionSaver.save(currentEventBinLogPosition);
        }
    }

    private boolean rowsEventDataIncluded(TableInfo tableInfo) {
        String dbTableName = tableInfo.getDatabase() + "." + tableInfo.getTable();
        Boolean excluded = excludeFilters.getOrDefault(dbTableName, Boolean.FALSE);
        Boolean included = includeFilters.isEmpty() || includeFilters.getOrDefault(dbTableName, Boolean.FALSE);
        return !excluded && included;
    }

    private DataRow convertDataRow(List<String> colNames, List<FieldType> incColTypes, Object[] row) {
        BinLogReaderDataRow dataRow = new BinLogReaderDataRow();
        for (int i = 0; i < row.length; i++) {
            dataRow.add(colNames.isEmpty() ? "col" + i : colNames.get(i),
                    incColTypes.isEmpty() ? null : incColTypes.get(i),
                    row[i]);
        }
        dataRow.setHasTableColumnNames(colNames.size() > 0);
        return dataRow;
    }

    @Override
    public void onUpdateRowsEvent(BinLogHeader header, UpdateRowsEvent data) {
        handleRowsEvent(header, data, new RowchangeDataFactory() {
            @Override
            public List<RowChangedData> create(BinlogPosition binlogPosition, TableInfo tableInfo, List<String> colNames) {
                List<String> incColNames = includedColumnNames(colNames, data.getColumnUsed());
                List<FieldType> incColTypes = includedColumnTypes(tableInfo.getColumnTypes(), data.getColumnUsed());
                List<String> incUpdColNames = includedColumnNames(colNames, data.getUpdateColumnUsed());
                List<FieldType> incUpdColTypes = includedColumnTypes(tableInfo.getColumnTypes(), data.getUpdateColumnUsed());

                List<RowChangedData> rowChangedDataList = data.getPairs().stream()
                        .map(rowPair -> new RowChangedData(
                                ChangeType.UPDATE,
                                tableInfo.getDatabase(),
                                tableInfo.getTable(),
                                header.getTimestamp(),
                                convertDataRow(incUpdColNames, incUpdColTypes, rowPair.getAfter()),
                                convertDataRow(incColNames, incColTypes, rowPair.getBefore()),
                                binlogPosition
                        ))
                        .collect(Collectors.toList());
                return rowChangedDataList;
            }
        });
    }

    @Override
    public void onDeleteRowsEvent(BinLogHeader header, DeleteRowsEvent data) {
        handleRowsEvent(header, data, new RowchangeDataFactory() {
            @Override
            public List<RowChangedData> create(BinlogPosition binlogPosition, TableInfo tableInfo, List<String> colNames) {
                List<String> incColNames = includedColumnNames(colNames, data.getColumnUsed());
                List<FieldType> incColTypes = includedColumnTypes(tableInfo.getColumnTypes(), data.getColumnUsed());

                List<RowChangedData> rowChangedDataList = data.getRows().stream()
                        .map(row -> new RowChangedData(
                                ChangeType.DELETE,
                                tableInfo.getDatabase(),
                                tableInfo.getTable(),
                                header.getTimestamp(),
                                convertDataRow(incColNames, incColTypes, row),
                                binlogPosition
                        ))
                        .collect(Collectors.toList());
                return rowChangedDataList;
            }
        });
    }

    @Override
    public void onXidEvent(BinLogHeader header, XidEvent data) {
        final BinlogPosition currentEventBinLogPosition =
                new BinlogPosition(currentBinlogFilename, header.getNextPosition());
        try {
            listener.onXid(data.getXid());
        } finally {
            binlogPositionSaver.save(currentEventBinLogPosition);
        }
    }

    @Override
    public void onHeartbeatEvent(BinLogHeader header, HeartbeatEvent data) {
        BinLogListener.super.onHeartbeatEvent(header, data);
    }

    @Override
    public void onStopEvent(BinLogHeader header, StopEvent data) {
        listener.stopped();
    }

    public void setIncludeFilters(String... filters) {
        this.includeFilters = new HashMap<>();
        if (filters != null) {
            for (String filter : filters) {
                includeFilters.put(filter, Boolean.TRUE);
            }
        }
    }

    public void setExcludeFilters(String... filters) {
        this.excludeFilters = new HashMap<>();
        if (filters != null) {
            for (String filter : filters) {
                excludeFilters.put(filter, Boolean.TRUE);
            }
        }
    }
}
