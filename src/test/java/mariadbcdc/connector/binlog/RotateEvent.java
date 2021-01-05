package mariadbcdc.connector.binlog;

import mariadbcdc.connector.binlog.BinLogData;

public class RotateEvent implements BinLogData {
    private final long position;
    private final String filename;

    public RotateEvent(long position, String filename) {
        this.position = position;
        this.filename = filename;
    }

    public long getPosition() {
        return position;
    }

    public String getFilename() {
        return filename;
    }

    @Override
    public String toString() {
        return "RotateEvent{" +
                "position=" + position +
                ", filename='" + filename + '\'' +
                '}';
    }
}
