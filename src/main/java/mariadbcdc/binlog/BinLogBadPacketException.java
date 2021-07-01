package mariadbcdc.binlog;

public class BinLogBadPacketException extends BinLogException {
    public BinLogBadPacketException(String message) {
        super(message);
    }
}
