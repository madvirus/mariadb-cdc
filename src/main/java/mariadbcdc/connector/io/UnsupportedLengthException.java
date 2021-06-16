package mariadbcdc.connector.io;

import mariadbcdc.connector.BinLogException;

public class UnsupportedLengthException extends BinLogException {
    public UnsupportedLengthException(long len) {
        super("length: " + len);
    }
}
