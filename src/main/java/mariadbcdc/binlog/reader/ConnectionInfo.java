package mariadbcdc.binlog.reader;

public class ConnectionInfo {
    private final String host;
    private final int port;
    private final String user;
    private final String password;

    public ConnectionInfo(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
