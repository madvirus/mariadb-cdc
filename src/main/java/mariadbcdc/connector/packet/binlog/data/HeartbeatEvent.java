package mariadbcdc.connector.packet.binlog.data;

import mariadbcdc.connector.packet.binlog.BinLogData;

public class HeartbeatEvent implements BinLogData {
    private String binlogName;

    public HeartbeatEvent(String binlogName) {
        this.binlogName = binlogName;
    }

    public String getBinlogName() {
        return binlogName;
    }

    @Override
    public String toString() {
        return "HeartbeatEvent{" +
                "binlogName='" + binlogName + '\'' +
                '}';
    }
}
