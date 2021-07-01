package mariadbcdc.binlog.reader;

public class BinLogErrException extends BinLogException {
    public BinLogErrException(String msg) {
        super(msg);
    }
}
