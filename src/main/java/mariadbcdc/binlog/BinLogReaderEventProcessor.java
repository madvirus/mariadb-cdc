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
    private int localDateTimeAdjustingHour;

    private Map<String, Boolean> includeFilters = Collections.emptyMap();
    private Map<String, Boolean> excludeFilters = Collections.emptyMap();

    private String currentBinlogFilename;

    private String database;
    private String table;
    private FieldType[] columnTypes;
    private List<String> columnNamesOfMetadata;

    public BinLogReaderEventProcessor(MariadbCdcListener listener,
                                      CurrentBinlogFilenameGetter currentBinlogFilenameGetter,
                                      BinlogPositionSaver binlogPositionSaver,
                                      ColumnNamesGetter columnNamesGetter,
                                      SchemaChangeListener schemaChangeListener) {
        this(listener,
                currentBinlogFilenameGetter,
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangeListener,
                0);
    }

    public BinLogReaderEventProcessor(MariadbCdcListener listener,
                                      CurrentBinlogFilenameGetter currentBinlogFilenameGetter,
                                      BinlogPositionSaver binlogPositionSaver,
                                      ColumnNamesGetter columnNamesGetter,
                                      SchemaChangeListener schemaChangeListener,
                                      int localDateTimeAdjustingHour) {
        this.listener = listener;
        this.currentBinlogFilenameGetter = currentBinlogFilenameGetter;
        this.binlogPositionSaver = binlogPositionSaver;
        this.columnNamesGetter = columnNamesGetter;
        this.schemaChangeListener = schemaChangeListener;
        this.localDateTimeAdjustingHour = localDateTimeAdjustingHour;
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
        database = tableData.getDatabaseName();
        table = tableData.getTableName();
        columnTypes = tableData.getFieldTypes();
        columnNamesOfMetadata = tableData.getFullMeta()
                .map(fm -> fm.getColumnNames())
                .filter(names -> names != null && !names.isEmpty())
                .orElse(null);
    }

    private boolean hasPrecededDatabaseTableName() {
        return database != null && database.length() > 0 && table != null && table.length() > 0;
    }

    private boolean rowsEventDataIncluded() {
        String dbTableName = database + "." + table;
        Boolean excluded = excludeFilters.getOrDefault(dbTableName, Boolean.FALSE);
        Boolean included = includeFilters.isEmpty() || includeFilters.getOrDefault(dbTableName, Boolean.FALSE);
        return !excluded && included;
    }

    private List<String> getColumnNames() {
        return columnNamesOfMetadata != null ? columnNamesOfMetadata :
                hasPrecededDatabaseTableName() ? columnNamesGetter.getColumnNames(database, table) :
                        Collections.emptyList();
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
        final BinlogPosition currentEventBinLogPosition =
                new BinlogPosition(currentBinlogFilename, header.getNextPosition());
        try {
            if (hasPrecededDatabaseTableName() && rowsEventDataIncluded()) {
                List<String> colNames = getColumnNames();
                if (colNames.size() > 0 && colNames.size() != columnTypes.length) {
                    colNames = Collections.emptyList();
                }

                List<String> incColNames = includedColumnNames(colNames, data.getColumnUsed());
                List<FieldType> incColTypes = includedColumnTypes(this.columnTypes, data.getColumnUsed());

                List<RowChangedData> rowChangedDataList = data.getRows().stream()
                        .map(row -> new RowChangedData(
                                ChangeType.INSERT,
                                database,
                                table,
                                header.getTimestamp(),
                                convertDataRow(incColNames, incColTypes, row),
                                currentEventBinLogPosition
                        ))
                        .collect(Collectors.toList());
                listener.onDataChanged(rowChangedDataList);
            }
        } finally {
            binlogPositionSaver.save(currentEventBinLogPosition);
        }
    }

    private DataRow convertDataRow(List<String> colNames, List<FieldType> incColTypes, Object[] row) {
        BinLogReaderDataRow dataRow = new BinLogReaderDataRow(localDateTimeAdjustingHour);
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
        final BinlogPosition currentEventBinLogPosition =
                new BinlogPosition(currentBinlogFilename, header.getNextPosition());
        try {
            if (hasPrecededDatabaseTableName() && rowsEventDataIncluded()) {
                List<String> colNames = getColumnNames();
                if (colNames.size() > 0 && colNames.size() != columnTypes.length) {
                    colNames = Collections.emptyList();
                }

                List<String> incColNames = includedColumnNames(colNames, data.getColumnUsed());
                List<FieldType> incColTypes = includedColumnTypes(this.columnTypes, data.getColumnUsed());
                List<String> incUpdColNames = includedColumnNames(colNames, data.getUpdateColumnUsed());
                List<FieldType> incUpdColTypes = includedColumnTypes(this.columnTypes, data.getUpdateColumnUsed());

                List<RowChangedData> rowChangedDataList = data.getPairs().stream()
                        .map(rowPair -> new RowChangedData(
                                ChangeType.UPDATE,
                                database,
                                table,
                                header.getTimestamp(),
                                convertDataRow(incUpdColNames, incUpdColTypes, rowPair.getAfter()),
                                convertDataRow(incColNames, incColTypes, rowPair.getBefore()),
                                currentEventBinLogPosition
                        ))
                        .collect(Collectors.toList());
                listener.onDataChanged(rowChangedDataList);
            }
        } finally {
            binlogPositionSaver.save(currentEventBinLogPosition);
        }
    }

    @Override
    public void onDeleteRowsEvent(BinLogHeader header, DeleteRowsEvent data) {
        final BinlogPosition currentEventBinLogPosition =
                new BinlogPosition(currentBinlogFilename, header.getNextPosition());
        try {
            if (hasPrecededDatabaseTableName() && rowsEventDataIncluded()) {
                List<String> colNames = getColumnNames();
                if (colNames.size() > 0 && colNames.size() != columnTypes.length) {
                    colNames = Collections.emptyList();
                }

                List<String> incColNames = includedColumnNames(colNames, data.getColumnUsed());
                List<FieldType> incColTypes = includedColumnTypes(this.columnTypes, data.getColumnUsed());

                List<RowChangedData> rowChangedDataList = data.getRows().stream()
                        .map(row -> new RowChangedData(
                                ChangeType.DELETE,
                                database,
                                table,
                                header.getTimestamp(),
                                convertDataRow(incColNames, incColTypes, row),
                                currentEventBinLogPosition
                        ))
                        .collect(Collectors.toList());
                listener.onDataChanged(rowChangedDataList);
            }
        } finally {
            binlogPositionSaver.save(currentEventBinLogPosition);
        }
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
