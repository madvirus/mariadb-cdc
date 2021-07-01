package mariadbcdc.binlog.io;

import mariadbcdc.binlog.BinLogException;

public class BinLogEOFException extends BinLogException {
    public BinLogEOFException(String message) {
        super(message);
    }
}
