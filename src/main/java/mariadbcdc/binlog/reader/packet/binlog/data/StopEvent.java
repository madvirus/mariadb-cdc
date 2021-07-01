package mariadbcdc.binlog.reader.packet.binlog.data;

import mariadbcdc.binlog.reader.packet.binlog.BinLogData;

public class StopEvent implements BinLogData {

    @Override
    public String toString() {
        return "StopEvent{}";
    }
}
