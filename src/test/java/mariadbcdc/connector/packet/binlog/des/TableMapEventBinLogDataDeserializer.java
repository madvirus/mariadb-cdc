package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.FieldType;
import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;

import java.util.BitSet;
import java.util.Map;

public class TableMapEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        long tableId = readPacketData.readLong(6);
        readPacketData.skip(2); // reserved
        int lengthOfDatabaseName = readPacketData.readInt(1);
        String databaseName = readPacketData.readStringNul();
        int lengthOfTableName = readPacketData.readInt(1);
        String tableName = readPacketData.readStringNul();
        int numberOfColumns = readPacketData.readLengthEncodedInt();
        byte[] columnTypes = new byte[numberOfColumns];
        readPacketData.readBytes(columnTypes);
        FieldType[] fieldTypes = new FieldType[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            fieldTypes[i] = FieldType.byValue(Byte.toUnsignedInt(columnTypes[i]));
        }
        int lengthOfMetadata = readPacketData.readLengthEncodedInt();
        byte[] metadataBlock = new byte[lengthOfMetadata];
        readPacketData.readBytes(metadataBlock);
        int[] metadata = toMetaData(metadataBlock, fieldTypes);
        int bitfieldLen = readPacketData.remaining();
        byte[] bitField = new byte[bitfieldLen];
        readPacketData.readBytes(bitField);

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
                    metadata[i] = ByteUtils.toBigEndianInt(metadataBlock, blockIdx, 2);
                case BIT:
                case VARCHAR:
                case NEWDECIMAL:
                case DECIMAL:
                case VAR_STRING:
                    metadata[i] = ByteUtils.toLittleEndianInt(metadataBlock, blockIdx, 2);
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

}
