package mariadbcdc;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
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

    private String database;
    private String table;
    private byte[] columnTypes;
    private List<String> columnNamesOfMetadata;

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
        }
        if (header instanceof EventHeaderV4) {
            EventHeaderV4 hv4 = (EventHeaderV4) header;
            binlogPositionSaver.save(new BinlogPosition(currentBinlogFilename, hv4.getNextPosition()));
        }
        if (data instanceof TableMapEventData) {
            TableMapEventData tableData = (TableMapEventData) data;
            database = tableData.getDatabase();
            table = tableData.getTable();
            columnTypes = tableData.getColumnTypes();

            columnNamesOfMetadata = Optional.ofNullable(tableData.getEventMetadata())
                    .map(meta -> meta.getColumnNames())
                    .filter(col -> !col.isEmpty())
                    .orElse(null);
        }
        if (data instanceof QueryEventData) {
            handleQueryEventData((QueryEventData)data);
        }
        if (data instanceof WriteRowsEventData) {
            if (rowsEventDataIncluded()) {
                handleWriteRowsEventData((WriteRowsEventData) data);
            }
        }
        if (data instanceof UpdateRowsEventData) {
            if (rowsEventDataIncluded()) {
                handleUpdateRowsEventData((UpdateRowsEventData) data);
            }
        }
        if (data instanceof DeleteRowsEventData) {
            if (rowsEventDataIncluded()) {
                handleDeleteRowsEventData((DeleteRowsEventData) data);
            }
        }
        if (data instanceof XidEventData) {
            listener.onXid(((XidEventData) data).getXid());
        }
    }

    private void handleQueryEventData(QueryEventData data) {
        AlterQueryDecision decision = QueryDecider.decideAlterQuery(data.getSql());
        if (decision.isAlterQuery()) {
            String db = decision.hasDatabase() ? decision.getDatabase() : data.getDatabase();
            schemaChangeListener.onSchemaChanged(new SchemaChangedData(db, decision.getTable()));
        }
    }

    private boolean rowsEventDataIncluded() {
        String dbTableName = database + "." + table;
        Boolean excluded = excludeFilters.getOrDefault(dbTableName, Boolean.FALSE);
        Boolean included = includeFilters.isEmpty() || includeFilters.getOrDefault(dbTableName, Boolean.FALSE);
        return !excluded && included;
    }

    private List<String> getColumnNames() {
        return columnNamesOfMetadata != null ? columnNamesOfMetadata : columnNamesGetter.getColumnNames(database, table);
    }

    private void handleWriteRowsEventData(WriteRowsEventData rowData) {
        List<String> colNames = getColumnNames();
        List<String> incColNames = includedColumnNames(colNames, rowData.getIncludedColumns());
        List<ColumnType> incColTypes = includedColumnTypes(this.columnTypes, rowData.getIncludedColumns());

        List<RowChangedData> rowChangedDataList = rowData.getRows().stream()
                .map(row -> new RowChangedData(
                        ChangeType.INSERT,
                        database,
                        table,
                        convertDataRow(incColNames, incColTypes, row)
                ))
                .collect(Collectors.toList());
        listener.onDataChanged(rowChangedDataList);
    }

    private void handleUpdateRowsEventData(UpdateRowsEventData rowData) {
        List<String> colNames = getColumnNames();
        List<String> incColNames = includedColumnNames(colNames, rowData.getIncludedColumns());
        List<ColumnType> incColTypes = includedColumnTypes(this.columnTypes, rowData.getIncludedColumns());
        List<String> incColNamesBeforeUpdate = includedColumnNames(colNames, rowData.getIncludedColumnsBeforeUpdate());
        List<ColumnType> incColTypesBeforeUpdate = includedColumnTypes(this.columnTypes, rowData.getIncludedColumnsBeforeUpdate());

        List<RowChangedData> rowChangedDataList = rowData.getRows().stream()
                .map(row -> new RowChangedData(
                        ChangeType.UPDATE,
                        database,
                        table,
                        convertDataRow(incColNames, incColTypes, row.getValue()),
                        convertDataRow(incColNamesBeforeUpdate, incColTypesBeforeUpdate, row.getKey())
                ))
                .collect(Collectors.toList());
        listener.onDataChanged(rowChangedDataList);
    }

    private void handleDeleteRowsEventData(DeleteRowsEventData rowData) {
        List<String> colNames = getColumnNames();
        List<String> incColNames = includedColumnNames(colNames, rowData.getIncludedColumns());
        List<ColumnType> incColTypes = includedColumnTypes(this.columnTypes, rowData.getIncludedColumns());

        List<RowChangedData> rowChangedDataList = rowData.getRows().stream()
                .map(row -> new RowChangedData(
                        ChangeType.DELETE,
                        database,
                        table,
                        convertDataRow(incColNames, incColTypes, row)
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
        for (int i = 0 ; i < row.length ; i++) {
            dataRow.add(colNames.isEmpty() ? "col" + i : colNames.get(i),
                    incColTypes.isEmpty() ? null : incColTypes.get(i),
                    row[i]);
        }
        return dataRow;
    }

    public void setIncludeFilters(String ... filters) {
        this.includeFilters = new HashMap<>();
        if (filters != null) {
            for (String filter : filters) {
                includeFilters.put(filter, Boolean.TRUE);
            }
        }
    }

    public void setExcludeFilters(String ... filters) {
        this.excludeFilters = new HashMap<>();
        if (filters != null) {
            for (String filter : filters) {
                excludeFilters.put(filter, Boolean.TRUE);
            }
        }
    }
}
