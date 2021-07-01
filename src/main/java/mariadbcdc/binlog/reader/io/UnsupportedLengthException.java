package mariadbcdc.binlog.reader.io;

import mariadbcdc.binlog.reader.BinLogException;

public class UnsupportedLengthException extends BinLogException {
    public UnsupportedLengthException(long len) {
        super("length: " + len);
    }
}
