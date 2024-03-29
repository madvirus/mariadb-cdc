package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import mariadbcdc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BinaryLogEventProcessor implements BinaryLogClient.EventListener {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private MariadbCdcListener listener;
    private CurrentBinlogFilenameGetter currentBinlogFilenameGetter;
    private BinlogPositionSaver binlogPositionSaver;
    private ColumnNamesGetter columnNamesGetter;
    private SchemaChangeListener schemaChangeListener;

    private Map<String, Boolean> includeFilters = Collections.emptyMap();
    private Map<String, Boolean> excludeFilters = Collections.emptyMap();

    private String currentBinlogFilename;

    private TableInfos tableInfos = new TableInfos();

    public BinaryLogEventProcessor(MariadbCdcListener listener,
                                   CurrentBinlogFilenameGetter currentBinlogFilenameGetter,
                                   BinlogPositionSaver binlogPositionSaver,
                                   ColumnNamesGetter columnNamesGetter,
                                   SchemaChangeListener schemaChangeListener) {
        this.currentBinlogFilenameGetter = currentBinlogFilenameGetter;
        this.listener = listener;
        this.binlogPositionSaver = binlogPositionSaver;
        this.columnNamesGetter = columnNamesGetter;
        this.schemaChangeListener = schemaChangeListener;
    }

    @Override
    public void onEvent(Event event) {
        if (currentBinlogFilename == null)
            currentBinlogFilename = currentBinlogFilenameGetter.getCurrentBinlogFilename();
        EventHeader header = event.getHeader();
        EventData data = event.getData();
        logger.debug("binlog header: {}", header);
        logger.debug("binlog data: {}", data);

        if (data instanceof RotateEventData) {
            RotateEventData r = (RotateEventData) data;
            currentBinlogFilename = r.getBinlogFilename();
            binlogPositionSaver.save(new BinlogPosition(r.getBinlogFilename(), r.getBinlogPosition()));
            return;
        }
        BinlogPosition currentEventBinLogPosition = null;
        if (header instanceof EventHeaderV4) {
            if (isBinPositionSaveTarget(data)) {
                EventHeaderV4 hv4 = (EventHeaderV4) header;
                currentEventBinLogPosition = new BinlogPosition(currentBinlogFilename, hv4.getNextPosition());
            }
        }

        try {
            if (data instanceof QueryEventData) {
                handleQueryEventData((QueryEventData) data);
            }
            if (data instanceof TableMapEventData) {
                TableMapEventData tableData = (TableMapEventData) data;
                tableInfos.add(new TableInfo(tableData.getTableId(),
                        tableData.getDatabase(), tableData.getTable(),
                        tableData.getColumnTypes(),
                        Optional.ofNullable(tableData.getEventMetadata())
                                .map(meta -> meta.getColumnNames())
                                .filter(col -> !col.isEmpty())
                                .orElse(null)
                ));
            }
            if (data instanceof WriteRowsEventData ||
                    data instanceof UpdateRowsEventData ||
                    data instanceof DeleteRowsEventData) {
                long tableId = getTableId(data);
                TableInfo tableInfo = tableInfos.getTableInfo(tableId);
                if (tableInfo != null && tableInfo.hasDatabaseTableName()) {
                    if (rowsEventDataIncluded(tableInfo.getDatabase(), tableInfo.getTable())) {
                        List<String> colNames = getColumnNames(tableInfo);
                        if (colNames.size() > 0 && colNames.size() != tableInfo.getColumnTypes().length) {
                            colNames = Collections.emptyList();
                        }
                        if (data instanceof WriteRowsEventData) {
                            handleWriteRowsEventData(currentEventBinLogPosition, header, tableInfo, (WriteRowsEventData) data, colNames);
                        } else if (data instanceof UpdateRowsEventData) {
                            handleUpdateRowsEventData(currentEventBinLogPosition, header, tableInfo, (UpdateRowsEventData) data, colNames);
                        } else if (data instanceof DeleteRowsEventData) {
                            handleDeleteRowsEventData(currentEventBinLogPosition, header, tableInfo, (DeleteRowsEventData) data, colNames);
                        }
                    }
                }
            }

            if (data instanceof XidEventData) {
                listener.onXid(currentEventBinLogPosition, ((XidEventData) data).getXid());
            }
        } catch (Exception ex) {
            // ignore listener thrown exception
            logger.warn("listener thrown exception: " + ex.getMessage(), ex);
        } finally {
            if (currentEventBinLogPosition != null) {
                binlogPositionSaver.save(currentEventBinLogPosition);
            }
        }
    }

    private long getTableId(EventData data) {
        if (data instanceof WriteRowsEventData) {
            return ((WriteRowsEventData) data).getTableId();
        } else if (data instanceof UpdateRowsEventData) {
            return ((UpdateRowsEventData) data).getTableId();
        } else if (data instanceof DeleteRowsEventData) {
            return ((DeleteRowsEventData) data).getTableId();
        } else {
            return -1;
        }
    }

    private boolean isBinPositionSaveTarget(EventData data) {
        if (data instanceof FormatDescriptionEventData) return false;
        if (data instanceof TableMapEventData) return false;
        return true;
    }

    private void handleQueryEventData(QueryEventData data) {
        SchemaChangeQueryDecision decision = QueryDecider.decideSchemaChangeQuery(data.getSql());
        if (decision.isAlterQuery()) {
            decision.getDatabaseTableNames()
                    .forEach(schemaChangedTable -> {
                        schemaChangeListener.onSchemaChanged(schemaChangedTable);
                    });
        }
    }

    private boolean rowsEventDataIncluded(String database, String table) {
        String dbTableName = database + "." + table;
        Boolean excluded = excludeFilters.getOrDefault(dbTableName, Boolean.FALSE);
        Boolean included = includeFilters.isEmpty() || includeFilters.getOrDefault(dbTableName, Boolean.FALSE);
        return !excluded && included;
    }

    private List<String> getColumnNames(TableInfo tableInfo) {
        if (tableInfo.getColumnNamesOfMetadata() != null) return tableInfo.getColumnNamesOfMetadata();
        if (tableInfo.hasDatabaseTableName())
            return columnNamesGetter.getColumnNames(tableInfo.getDatabase(), tableInfo.getTable());
        return Collections.emptyList();
    }

    private void handleWriteRowsEventData(BinlogPosition currentEventBinLogPosition, EventHeader header, TableInfo tableInfo, WriteRowsEventData rowData, List<String> colNames) {
        List<String> incColNames = includedColumnNames(colNames, rowData.getIncludedColumns());
        List<ColumnType> incColTypes = includedColumnTypes(tableInfo.getColumnTypes(), rowData.getIncludedColumns());

        List<RowChangedData> rowChangedDataList = rowData.getRows().stream()
                .map(row -> new RowChangedData(
                        ChangeType.INSERT,
                        tableInfo.getDatabase(),
                        tableInfo.getTable(),
                        header.getTimestamp(),
                        convertDataRow(incColNames, incColTypes, row),
                        currentEventBinLogPosition
                ))
                .collect(Collectors.toList());
        listener.onDataChanged(rowChangedDataList);
    }

    private void handleUpdateRowsEventData(BinlogPosition currentEventBinLogPosition, EventHeader header, TableInfo tableInfo, UpdateRowsEventData rowData, List<String> colNames) {
        List<String> incColNames = includedColumnNames(colNames, rowData.getIncludedColumns());
        List<ColumnType> incColTypes = includedColumnTypes(tableInfo.getColumnTypes(), rowData.getIncludedColumns());
        List<String> incColNamesBeforeUpdate = includedColumnNames(colNames, rowData.getIncludedColumnsBeforeUpdate());
        List<ColumnType> incColTypesBeforeUpdate = includedColumnTypes(tableInfo.getColumnTypes(), rowData.getIncludedColumnsBeforeUpdate());

        List<RowChangedData> rowChangedDataList = rowData.getRows().stream()
                .map(row -> new RowChangedData(
                        ChangeType.UPDATE,
                        tableInfo.getDatabase(),
                        tableInfo.getTable(),
                        header.getTimestamp(),
                        convertDataRow(incColNames, incColTypes, row.getValue()),
                        convertDataRow(incColNamesBeforeUpdate, incColTypesBeforeUpdate, row.getKey()),
                        currentEventBinLogPosition
                ))
                .collect(Collectors.toList());
        listener.onDataChanged(rowChangedDataList);
    }

    private void handleDeleteRowsEventData(BinlogPosition currentEventBinLogPosition, EventHeader header, TableInfo tableInfo, DeleteRowsEventData rowData, List<String> colNames) {
        List<String> incColNames = includedColumnNames(colNames, rowData.getIncludedColumns());
        List<ColumnType> incColTypes = includedColumnTypes(tableInfo.getColumnTypes(), rowData.getIncludedColumns());

        List<RowChangedData> rowChangedDataList = rowData.getRows().stream()
                .map(row -> new RowChangedData(
                        ChangeType.DELETE,
                        tableInfo.getDatabase(),
                        tableInfo.getTable(),
                        header.getTimestamp(),
                        convertDataRow(incColNames, incColTypes, row),
                        currentEventBinLogPosition
                ))
                .collect(Collectors.toList());
        listener.onDataChanged(rowChangedDataList);
    }

    private List<String> includedColumnNames(List<String> colNames, BitSet includedColumns) {
        if (colNames == null || colNames.isEmpty()) {
            return Collections.emptyList();
        }
        return IntStream.range(0, colNames.size()).filter(i -> includedColumns.get(i))
                .mapToObj(i -> colNames.get(i))
                .collect(Collectors.toList());
    }

    private List<ColumnType> includedColumnTypes(byte[] columnTypes, BitSet includedColumns) {
        return IntStream.range(0, columnTypes.length).filter(i -> includedColumns.get(i))
                .mapToObj(i -> ColumnType.byCode(Byte.toUnsignedInt(columnTypes[i])))
                .collect(Collectors.toList());
    }

    private DataRow convertDataRow(List<String> colNames, List<ColumnType> incColTypes, Serializable[] row) {
        DataRowImpl dataRow = new DataRowImpl();
        for (int i = 0; i < row.length; i++) {
            dataRow.add(colNames.isEmpty() ? "col" + i : colNames.get(i),
                    incColTypes.isEmpty() ? null : incColTypes.get(i),
                    row[i]);
        }
        dataRow.setHasTableColumnNames(colNames.size() > 0);
        return dataRow;
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
