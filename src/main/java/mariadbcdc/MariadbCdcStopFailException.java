package mariadbcdc;

public class MariadbCdcStopFailException extends RuntimeException {
    public MariadbCdcStopFailException(Throwable e) {
        super(e);
    }
}

