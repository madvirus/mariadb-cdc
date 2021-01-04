package mariadbcdc.connector.binlog;

public class BinLogHeader {
    private long timestamp;
    private BinlogEventType eventType;
    private long serverId;
    private long eventLength;
    private long nextPosition;
    private int flags;

    public BinLogHeader(long timestamp,
                        BinlogEventType eventType,
                        long serverId,
                        long eventLength,
                        long nextPosition,
                        int flags) {
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.serverId = serverId;
        this.eventLength = eventLength;
        this.nextPosition = nextPosition;
        this.flags = flags;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public BinlogEventType getEventType() {
        return eventType;
    }

    public long getServerId() {
        return serverId;
    }

    public long getEventLength() {
        return eventLength;
    }

    public long getNextPosition() {
        return nextPosition;
    }

    public int getFlags() {
        return flags;
    }

    @Override
    public String toString() {
        return "BinLogHeader{" +
                "timestamp=" + timestamp +
                ", eventType=" + eventType +
                ", serverId=" + serverId +
                ", eventLength=" + eventLength +
                ", nextPosition=" + nextPosition +
                ", flags=" + flags +
                '}';
    }
}
