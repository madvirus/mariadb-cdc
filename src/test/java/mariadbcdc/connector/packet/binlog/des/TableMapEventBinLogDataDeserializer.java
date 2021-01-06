package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.handler.TableMapEvent;
import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;

public class TableMapEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(PacketIO packetIO, BinLogStatus binLogStatus, BinLogHeader header) {
        long tableId = packetIO.readLong(6);
        packetIO.skip(2); // reserved
        int lengthOfDatabaseName = packetIO.readInt(1);
        String databaseName = packetIO.readStringNul();
        int lengthOfTableName = packetIO.readInt(1);
        String tableName = packetIO.readStringNul();
        int numberOfColumns = packetIO.readLengthEncodedInt();
        byte[] columnTypes = packetIO.readBytes(new byte[numberOfColumns], 0, numberOfColumns);
        int lengthOfMetadata = packetIO.readLengthEncodedInt();
        byte[] metadata = packetIO.readBytes(new byte[lengthOfMetadata], 0, lengthOfMetadata);
        int bitfieldLen = packetIO.remainingBlock();
        byte[] bitField = packetIO.readBytes(new byte[bitfieldLen], 0, bitfieldLen);
        TableMapEvent tableMapEvent = new TableMapEvent(
                tableId,
                lengthOfDatabaseName,
                databaseName,
                lengthOfTableName,
                tableName,
                numberOfColumns,
                columnTypes,
                lengthOfMetadata,
                metadata,
                bitField
        );
        return new TableMapEvent(
                tableId,
                lengthOfDatabaseName,
                databaseName,
                lengthOfTableName,
                tableName,
                numberOfColumns,
                columnTypes,
                lengthOfMetadata,
                metadata,
                bitField
        );
    }
}
