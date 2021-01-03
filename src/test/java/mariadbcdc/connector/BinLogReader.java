package mariadbcdc.connector;

import mariadbcdc.BinlogPosition;

public class BinLogReader {
    private final String host;
    private final int port;
    private final String user;
    private final String password;

    private BinLogSession session;

    private boolean reading = false;

    public BinLogReader(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public void connect() {
        session = new BinLogSession(host, port, user, password);
        session.handshake();
        session.fetchBinlogFilenameAndPosition();
    }

    public void start() {
        session.registerSlave();
        read();
    }

    public void read() {
        reading = true;
        try {
            while (reading) {
                session.readBinlog();
            }
        } finally {
            reading = false;
        }
    }

    public void stop() {
        reading = false;
    }

    public BinlogPosition getPosition() {
        return new BinlogPosition(session.getBinlogFile(), session.getBinlogPosition());
    }

    public void disconnect() {
        if (reading) reading = false;
        session.close();
    }

}
