package mariadbcdc.binlog.io;

import mariadbcdc.binlog.BinLogException;

public class UnsupportedLengthException extends BinLogException {
    public UnsupportedLengthException(long len) {
        super("length: " + len);
    }
}
