package mariadbcdc.connector.io;

import mariadbcdc.connector.BinLogException;

public class BinLogEOFException extends BinLogException {
    public BinLogEOFException(String message) {
        super(message);
    }
}
