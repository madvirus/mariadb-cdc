package mariadbcdc.binlog.reader.packet.binlog.des;

import mariadbcdc.binlog.reader.io.ReadPacketData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogData;
import mariadbcdc.binlog.reader.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.reader.packet.binlog.BinLogStatus;
import mariadbcdc.binlog.reader.packet.binlog.data.QueryEvent;
import mariadbcdc.binlog.reader.packet.binlog.data.TableMapEvent;

import java.util.Map;

public class QueryEventBinLogDataDeserializer implements BinLogDataDeserializer {
    @Override
    public BinLogData deserialize(ReadPacketData readPacketData, BinLogStatus binLogStatus, BinLogHeader header, Map<Long, TableMapEvent> tableMap) {
        long threadId = readPacketData.readLong(4);
        long executeTime = readPacketData.readLong(4);
        int lengthOfDatabaseName = readPacketData.readInt(1);
        int errorCode = readPacketData.readInt(2);
        int lengthOfVariableBlock = readPacketData.readInt(2);
        byte[] statusVariables = new byte[lengthOfVariableBlock];
        readPacketData.readBytes(statusVariables);
        String defaultDatabase = readPacketData.readStringNul();
        String sql = readPacketData.readStringEOF();

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
