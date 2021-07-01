package mariadbcdc.binlog.io;

import mariadbcdc.binlog.BinLogException;

public class BinLogIOException extends BinLogException {
    public BinLogIOException(Throwable cause) {
        super(cause);
    }
}
