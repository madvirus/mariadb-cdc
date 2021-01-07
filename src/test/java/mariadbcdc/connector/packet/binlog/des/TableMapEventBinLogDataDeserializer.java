package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.FieldType;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;

import java.util.BitSet;
import java.util.Map;

public class TableMapEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(PacketIO packetIO, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        long tableId = packetIO.readLong(6);
        packetIO.skip(2); // reserved
        int lengthOfDatabaseName = packetIO.readInt(1);
        String databaseName = packetIO.readStringNul();
        int lengthOfTableName = packetIO.readInt(1);
        String tableName = packetIO.readStringNul();
        int numberOfColumns = packetIO.readLengthEncodedInt();
        byte[] columnTypes = packetIO.readBytes(new byte[numberOfColumns], 0, numberOfColumns);
        FieldType[] fieldTypes = new FieldType[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            fieldTypes[i] = FieldType.byValue(Byte.toUnsignedInt(columnTypes[i]));
        }
        int lengthOfMetadata = packetIO.readLengthEncodedInt();
        byte[] metadataBlock = packetIO.readBytes(new byte[lengthOfMetadata], 0, lengthOfMetadata);
        int[] metadata = toMetaData(metadataBlock, fieldTypes);
        int bitfieldLen = packetIO.remainingBlock();
        byte[] bitField = packetIO.readBytes(new byte[bitfieldLen], 0, bitfieldLen);

        return new TableMapEvent(
                tableId,
                lengthOfDatabaseName,
                databaseName,
                lengthOfTableName,
                tableName,
                numberOfColumns,
                fieldTypes,
                lengthOfMetadata,
                metadata,
                BitSet.valueOf(bitField)
        );
    }

    private int[] toMetaData(byte[] metadataBlock, FieldType[] fieldTypes) {
        int[] metadata = new int[fieldTypes.length];
        int blockIdx = 0;
        for (int i = 0; i < fieldTypes.length; i++) {
            // https://mariadb.com/kb/en/rows_event_v1/
            int len = 0;
            switch (fieldTypes[i]) {
                case ENUM:
                case SET:
                case STRING:
                    metadata[i] = toBigEndianInt(metadataBlock, blockIdx, 2);
                case BIT:
                case VARCHAR:
                case NEWDECIMAL:
                case DECIMAL:
                case VAR_STRING:
                    metadata[i] = toInt(metadataBlock, blockIdx, 2);
                    blockIdx += 2;
                    break;
                case TINY_BLOB:
                case MEDIUM_BLOB:
                case LONG_BLOB:
                case BLOB:
                case FLOAT:
                case DOUBLE:
                case TIMESTAMP2:
                case DATETIME2:
                case TIME2:
                    metadata[i] = Byte.toUnsignedInt(metadataBlock[blockIdx]);
                    blockIdx++;
                    break;
                default:
                    metadata[i] = 0;
            }
        }
        return metadata;
    }

    private int toBigEndianInt(byte[] metadataBlock, int off, int len) {
        int value = 0;
        for (int i = 0 ; i < len ; i++) {
            value = (value << 8) | Byte.toUnsignedInt(metadataBlock[off + i]);
        }
        return value;
    }

    private int toInt(byte[] metadataBlock, int off, int len) {
        int value = 0;
        for (int i = 0 ; i < len ; i++) {
            value |= Byte.toUnsignedInt(metadataBlock[off + i]) << (i * 8);
        }
        return value;
    }
}
