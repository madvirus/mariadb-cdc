package mariadbcdc.connector;

public class BinLogBadPacketException extends BinLogException {
    public BinLogBadPacketException(String message) {
        super(message);
    }
}
