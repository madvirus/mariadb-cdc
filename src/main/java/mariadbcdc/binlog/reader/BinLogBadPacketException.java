package mariadbcdc.binlog.reader;

public class BinLogBadPacketException extends BinLogException {
    public BinLogBadPacketException(String message) {
        super(message);
    }
}
