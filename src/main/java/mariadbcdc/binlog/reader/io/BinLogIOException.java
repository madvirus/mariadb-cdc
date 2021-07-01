package mariadbcdc.binlog.reader.io;

import mariadbcdc.binlog.reader.BinLogException;

public class BinLogIOException extends BinLogException {
    public BinLogIOException(Throwable cause) {
        super(cause);
    }
}
