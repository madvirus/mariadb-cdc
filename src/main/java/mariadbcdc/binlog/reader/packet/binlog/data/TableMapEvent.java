package mariadbcdc.binlog.reader.packet.binlog.data;

import mariadbcdc.binlog.reader.FieldType;
import mariadbcdc.binlog.reader.packet.binlog.BinLogData;

import java.util.Arrays;
import java.util.BitSet;

public class TableMapEvent implements BinLogData {
    /** The table ID. */
    private final long tableId;
    private final String databaseName;
    private final String tableName;
    /** The number of columns in the table. */
    private final int numberOfColumns;
    /** An array of 'n' column types, one byte per column. */
    private final FieldType[] fieldTypes;
    /** The metadata block */
    private final int[] metadata;
    /** Bit-field indicating whether each column can be NULL, one bit per column. */
    private final BitSet nullability;

    public TableMapEvent(long tableId,
                         String databaseName,
                         String tableName,
                         int numberOfColumns,
                         FieldType[] fieldTypes,
                         int[] metadata,
                         BitSet nullability) {
        this.tableId = tableId;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.numberOfColumns = numberOfColumns;
        this.fieldTypes = fieldTypes;
        this.metadata = metadata;
        this.nullability = nullability;
    }

    public long getTableId() {
        return tableId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    public FieldType[] getFieldTypes() {
        return fieldTypes;
    }

    public int[] getMetadata() {
        return metadata;
    }

    public BitSet getNullability() {
        return nullability;
    }

    @Override
    public String toString() {
        return "TableMapEvent{" +
                "tableId=" + tableId +
                ", databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", numberOfColumns=" + numberOfColumns +
                ", fieldTypes=" + Arrays.toString(fieldTypes) +
                ", metadata=" + Arrays.toString(metadata) +
                ", nullability=" + nullability.toString() +
                '}';
    }
}
