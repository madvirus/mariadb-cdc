package mariadbcdc.binlog.packet.binlog;

public class BinLogHeader {
    private long timestamp;
    private int eventCode;
    private BinlogEventType eventType;
    private long serverId;
    private long eventLength;
    private long nextPosition;
    private int flags;

    public BinLogHeader(long timestamp,
                        int eventCode,
                        long serverId,
                        long eventLength,
                        long nextPosition,
                        int flags) {
        this.timestamp = timestamp;
        this.eventCode = eventCode;
        this.eventType = BinlogEventType.byCode(eventCode);
        this.serverId = serverId;
        this.eventLength = eventLength;
        this.nextPosition = nextPosition;
        this.flags = flags;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getEventCode() {
        return eventCode;
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

    public long getEventDataLength() {
        return eventLength - headerSize();
    }

    private long headerSize() {
        return 19;
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
                ", eventCode=" + eventCode +
                ", eventType=" + eventType +
                ", serverId=" + serverId +
                ", eventLength=" + eventLength +
                ", nextPosition=" + nextPosition +
                ", flags=" + flags +
                '}';
    }
}
