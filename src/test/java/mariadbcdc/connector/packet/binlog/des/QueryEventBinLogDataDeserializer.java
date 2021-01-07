package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.io.PacketIO;
import mariadbcdc.connector.packet.binlog.BinLogData;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.BinLogStatus;
import mariadbcdc.connector.packet.binlog.data.QueryEvent;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;

import java.util.Map;

public class QueryEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(PacketIO packetIO, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        long threadId = packetIO.readLong(4);
        long executeTime = packetIO.readLong(4);
        int lengthOfDatabaseName = packetIO.readInt(1);
        int errorCode = packetIO.readInt(2);
        int lengthOfVariableBlock = packetIO.readInt(2);
        byte[] statusVariables = new byte[lengthOfVariableBlock];
        packetIO.readBytes(statusVariables, 0, statusVariables.length);
        String defaultDatabase = packetIO.readStringNul();
        String sql = packetIO.readStringEOF();

        QueryEvent queryEvent = new QueryEvent(
                threadId,
                executeTime,
                lengthOfDatabaseName,
                errorCode,
                lengthOfVariableBlock,
                statusVariables,
                defaultDatabase,
                sql
        );
        return queryEvent;
    }
}
