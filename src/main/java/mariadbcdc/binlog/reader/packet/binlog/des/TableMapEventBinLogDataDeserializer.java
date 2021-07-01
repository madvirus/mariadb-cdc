package mariadbcdc.binlog.reader.packet.binlog.des;

import mariadbcdc.binlog.reader.FieldType;
import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.reader.packet.binlog.BinLogStatus;
import mariadbcdc.binlog.reader.packet.binlog.data.TableMapEvent;

import java.util.BitSet;
import java.util.Map;

public class TableMapEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        long tableId = readPacketData.readLong(6);
        readPacketData.skip(2); // reserved
        readPacketData.readInt(1); // skip lengthOfDatabaseName
        String databaseName = readPacketData.readStringNul();
        readPacketData.readInt(1); // skip lengthOfTableName
        String tableName = readPacketData.readStringNul();
        int numberOfColumns = readPacketData.readLengthEncodedInt();
        byte[] columnTypes = new byte[numberOfColumns];
        readPacketData.readBytes(columnTypes);
        FieldType[] fieldTypes = new FieldType[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            fieldTypes[i] = FieldType.byValue(Byte.toUnsignedInt(columnTypes[i]));
        }
        int lengthOfMetadata = readPacketData.readLengthEncodedInt();
        byte[] metadataBlock = readPacketData.readBytes(lengthOfMetadata);
        int[] metadata = toMetaData(metadataBlock, fieldTypes);
        int bitfieldLen = (numberOfColumns + 7) >> 3; // readPacketData.remaining();
        byte[] nullabilityBitField = readPacketData.readBytes(bitfieldLen);
        if (readPacketData.remaining() > 0) {

        }

        return new TableMapEvent(
                tableId,
                databaseName,
                tableName,
                numberOfColumns,
                fieldTypes,
                metadata,
                BitSet.valueOf(nullabilityBitField)
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
