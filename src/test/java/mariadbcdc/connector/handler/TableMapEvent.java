package mariadbcdc.connector.handler;

import mariadbcdc.connector.packet.binlog.BinLogData;

import java.util.Arrays;

public class TableMapEvent implements BinLogData {
    private final long tableId;
    private final int lengthOfDatabaseName;
    private final String databaseName;
    private final int lengthOfTableName;
    private final String tableName;
    private final int numberOfColumns;
    private final byte[] columnTypes;
    private final int lengthOfMetadata;
    private final byte[] metadata;
    private final byte[] bitField;

    public TableMapEvent(long tableId,
                         int lengthOfDatabaseName,
                         String databaseName,
                         int lengthOfTableName,
                         String tableName,
                         int numberOfColumns,
                         byte[] columnTypes,
                         int lengthOfMetadata,
                         byte[] metadata,
                         byte[] bitField) {
        this.tableId = tableId;
        this.lengthOfDatabaseName = lengthOfDatabaseName;
        this.databaseName = databaseName;
        this.lengthOfTableName = lengthOfTableName;
        this.tableName = tableName;
        this.numberOfColumns = numberOfColumns;
        this.columnTypes = columnTypes;
        this.lengthOfMetadata = lengthOfMetadata;
        this.metadata = metadata;
        this.bitField = bitField;
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

    public byte[] getColumnTypes() {
        return columnTypes;
    }

    public int getLengthOfMetadata() {
        return lengthOfMetadata;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public byte[] getBitField() {
        return bitField;
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
                ", columnTypes=" + Arrays.toString(columnTypes) +
                ", lengthOfMetadata=" + lengthOfMetadata +
                ", metadata=" + Arrays.toString(metadata) +
                ", bitField=" + Arrays.toString(bitField) +
                '}';
    }
}
