package mariadbcdc;

public class MariadbCdcStartFailException extends RuntimeException {
    public MariadbCdcStartFailException(Throwable e) {
        super(e);
    }
}

