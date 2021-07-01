package mariadbcdc.binlog.reader;

public class BinLogException extends RuntimeException {
    public BinLogException() {
        super();
    }

    public BinLogException(String message) {
        super(message);
    }

    public BinLogException(String message, Throwable cause) {
        super(message, cause);
    }

    public BinLogException(Throwable cause) {
        super(cause);
    }
}
