package mariadbcdc;

import java.time.Duration;

public class MariadbCdcConfig {
    private String host;
    private int port;
    private String user;
    private String password;
    private String positionTraceFile;
    private Duration heartbeatPeriod;
    private Long serverId;

    private String[] excludeFilters;
    private String[] includeFilters;

    private Class<? extends BinaryLogWrapperFactory> binaryLogWrapperFactoryClass;

    public MariadbCdcConfig(String host, int port, String user, String password, String positionTraceFile) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.positionTraceFile = positionTraceFile;
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

    public String getPositionTraceFile() {
        return positionTraceFile;
    }

    public Long getServerId() {
        return serverId;
    }

    public void setServerId(Long serverId) {
        this.serverId = serverId;
    }

    public void setExcludeFilters(String ... filters) {
        this.excludeFilters = filters;
    }

    public String[] getExcludeFilters() {
        return excludeFilters;
    }

    public void setIncludeFilters(String... filters) {
        this.includeFilters = filters;
    }

    public String[] getIncludeFilters() {
        return includeFilters;
    }

    public void setHeartbeatPeriod(Duration heartbeatPeriodSeconds) {
        this.heartbeatPeriod = heartbeatPeriod;
    }

    public Duration getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public Class<? extends BinaryLogWrapperFactory> getBinaryLogWrapperFactoryClass() {
        return binaryLogWrapperFactoryClass;
    }

    public void setBinaryLogWrapperFactoryClass(Class<? extends BinaryLogWrapperFactory> binaryLogWrapperFactoryClass) {
        this.binaryLogWrapperFactoryClass = binaryLogWrapperFactoryClass;
    }
}
