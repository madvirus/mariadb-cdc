package mariadbcdc.binlog.packet.binlog.data;

import mariadbcdc.binlog.packet.binlog.BinLogData;

public class StopEvent implements BinLogData {

    @Override
    public String toString() {
        return "StopEvent{}";
    }
}
