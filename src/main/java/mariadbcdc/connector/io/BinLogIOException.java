package mariadbcdc.connector.io;

import mariadbcdc.connector.BinLogException;

public class BinLogIOException extends BinLogException {
    public BinLogIOException(Throwable cause) {
        super(cause);
    }
}
