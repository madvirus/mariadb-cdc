package mariadbcdc;

public class MariadbCdcConfig {
    private String host;
    private int port;
    private String user;
    private String password;
    private String positionTraceFile;

    private Long serverId;

    private String[] excludeFilters;
    private String[] includeFilters;

    private int localDateTimeAdjustingHour = 0;

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

    public void setLocalDateTimeAdjustingHour(int localDateTimeAdjustingHour) {
        this.localDateTimeAdjustingHour = localDateTimeAdjustingHour;
    }

    public int getLocalDateTimeAdjustingHour() {
        return localDateTimeAdjustingHour;
    }
}
