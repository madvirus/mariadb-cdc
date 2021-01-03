package mariadbcdc.connector.io;

import mariadbcdc.connector.packet.ReadPacket;

public class ColumnDefPacket implements ReadPacket {
    private int sequenceNumber;

    private String catalog;
    private String schema;
    private String tableAlias;
    private String table;
    private String columnAlias;
    private String column;
    private int length;
    private int characterSet;
    private long maxColumnSize;
    private int fieldType;
    private int fieldDetailFlag;
    private int decimals;

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public String getTable() {
        return table;
    }

    public String getColumnAlias() {
        return columnAlias;
    }

    public String getColumn() {
        return column;
    }

    public int getLength() {
        return length;
    }

    public int getCharacterSet() {
        return characterSet;
    }

    public long getMaxColumnSize() {
        return maxColumnSize;
    }

    public int getFieldType() {
        return fieldType;
    }

    public int getFieldDetailFlag() {
        return fieldDetailFlag;
    }

    public int getDecimals() {
        return decimals;
    }

    public static ColumnDefPacket from(ReadPacketData readPacketData) {
        ColumnDefPacket packet = new ColumnDefPacket();
        packet.sequenceNumber = readPacketData.getSequenceNumber();
        packet.catalog = readPacketData.readLengthEncodedString();
        packet.schema = readPacketData.readLengthEncodedString();
        packet.tableAlias = readPacketData.readLengthEncodedString();
        packet.table = readPacketData.readLengthEncodedString();
        packet.columnAlias = readPacketData.readLengthEncodedString();
        packet.column = readPacketData.readLengthEncodedString();
        packet.length = readPacketData.readLengthEncodedInt();
        packet.characterSet = readPacketData.readInt(2);
        packet.maxColumnSize = readPacketData.readLong(4);
        packet.fieldType = readPacketData.readInt(1);
        packet.fieldDetailFlag = readPacketData.readInt(2);
        packet.decimals = readPacketData.readInt(1);
        readPacketData.readInt(2); // unused;
        return packet;
    }
}
