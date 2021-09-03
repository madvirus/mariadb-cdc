package mariadbcdc.binlog.reader;

public class StartedBadPositionException extends BinLogException {
    public StartedBadPositionException(String message) {
        super(message);
    }
}
