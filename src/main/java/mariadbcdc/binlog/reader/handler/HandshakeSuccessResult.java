package mariadbcdc.binlog.reader.handler;

public class HandshakeSuccessResult {
    private int protocolVersion;
    private String serverVersion;
    private int connectionId;

    private int serverCapabilities;
    private int clientCapabilities;

    public HandshakeSuccessResult(int protocolVersion,
                                  String serverVersion,
                                  int connectionId,
                                  int serverCapabilities,
                                  int clientCapabilities) {
        this.protocolVersion = protocolVersion;
        this.serverVersion = serverVersion;
        this.connectionId = connectionId;
        this.serverCapabilities = serverCapabilities;
        this.clientCapabilities = clientCapabilities;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public int getServerCapabilities() {
        return serverCapabilities;
    }

    public int getClientCapabilities() {
        return clientCapabilities;
    }
}
