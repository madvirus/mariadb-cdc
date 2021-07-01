package mariadbcdc.binlog.reader.io;

import mariadbcdc.binlog.reader.BinLogException;

public class BinLogEOFException extends BinLogException {
    public BinLogEOFException(String message) {
        super(message);
    }
}
