package mariadbcdc.binlog.packet.binlog;

public class BinLogEvent {
    public static final BinLogEvent EOF = new EOF();
    public static final BinLogEvent UNKNOWN = new Unknown();

    private BinLogHeader header;
    private BinLogData data;

    public BinLogEvent(BinLogHeader header, BinLogData data) {
        this.header = header;
        this.data = data;
    }

    public BinLogHeader getHeader() {
        return header;
    }

    public BinLogData getData() {
        return data;
    }

    public static class Unknown extends BinLogEvent {
        private Unknown() {
            super(null, null);
        }
    }

    public static class EOF extends BinLogEvent {

        private EOF() {
            super(null, null);
        }
    }
}
