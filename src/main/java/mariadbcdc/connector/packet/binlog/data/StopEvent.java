package mariadbcdc.connector.packet.binlog.data;

import mariadbcdc.connector.packet.binlog.BinLogData;

public class StopEvent implements BinLogData {

    @Override
    public String toString() {
        return "StopEvent{}";
    }
}
