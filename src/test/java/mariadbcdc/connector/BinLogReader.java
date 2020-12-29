package mariadbcdc.connector;

import mariadbcdc.BinlogPosition;

public class BinLogReader {
    private final String host;
    private final int port;
    private final String user;
    private final int password;

    public BinLogReader(String host, int port, String user, int password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public void connect() {

    }

    public BinlogPosition getPosition() {
        return null;
    }

    public void disconnect() {

    }
}
