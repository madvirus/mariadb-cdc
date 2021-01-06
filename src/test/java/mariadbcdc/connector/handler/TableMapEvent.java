package mariadbcdc.connector.handler;

import mariadbcdc.connector.FieldType;
import mariadbcdc.connector.packet.binlog.BinLogData;

import java.util.Arrays;
import java.util.BitSet;

public class TableMapEvent implements BinLogData {
    /** The table ID. */
    private final long tableId;
    /**  Database name length. */
    private final int lengthOfDatabaseName;
    /** The database name (null-terminated). */
    private final String databaseName;
    /** Table name length. */
    private final int lengthOfTableName;
    /** The table name (null-terminated). */
    private final String tableName;
    /** The number of columns in the table. */
    private final int numberOfColumns;
    /** An array of 'n' column types, one byte per column. */
    private final FieldType[] fieldTypes;
    /** The length of the metadata block. */
    private final int lengthOfMetadata;
    /** The metadata block */
    private final int[] metadata;
    /** Bit-field indicating whether each column can be NULL, one bit per column. */
    private final BitSet nullability;

    public TableMapEvent(long tableId,
                         int lengthOfDatabaseName,
                         String databaseName,
                         int lengthOfTableName,
                         String tableName,
                         int numberOfColumns,
                         FieldType[] fieldTypes,
                         int lengthOfMetadata,
                         int[] metadata,
                         BitSet nullability) {
        this.tableId = tableId;
        this.lengthOfDatabaseName = lengthOfDatabaseName;
        this.databaseName = databaseName;
        this.lengthOfTableName = lengthOfTableName;
        this.tableName = tableName;
        this.numberOfColumns = numberOfColumns;
        this.fieldTypes = fieldTypes;
        this.lengthOfMetadata = lengthOfMetadata;
        this.metadata = metadata;
        this.nullability = nullability;
    }

    public long getTableId() {
        return tableId;
    }

    public int getLengthOfDatabaseName() {
        return lengthOfDatabaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public int getLengthOfTableName() {
        return lengthOfTableName;
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

    public int getLengthOfMetadata() {
        return lengthOfMetadata;
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
                ", lengthOfDatabaseName=" + lengthOfDatabaseName +
                ", databaseName='" + databaseName + '\'' +
                ", lengthOfTableName=" + lengthOfTableName +
                ", tableName='" + tableName + '\'' +
                ", numberOfColumns=" + numberOfColumns +
                ", fieldTypes=" + Arrays.toString(fieldTypes) +
                ", lengthOfMetadata=" + lengthOfMetadata +
                ", metadata=" + Arrays.toString(metadata) +
                ", nullability=" + nullability.toString() +
                '}';
    }
}
