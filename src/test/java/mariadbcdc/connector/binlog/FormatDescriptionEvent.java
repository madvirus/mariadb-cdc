package mariadbcdc.connector.binlog;

public class FormatDescriptionEvent implements BinLogData {
    /**
     * uint<2> The binary log format version. This is 4 in MariaDB 10 and up.
     */
    private int logFormatVersion;
    /**
     * string<50> The MariaDB server version (example: 10.2.1-debug-log), padded with 0x00 bytes on the right.
     */
    private String serverVersion;
    /**
     * uint<4> Timestamp in milli when this event was created (this is the moment when the binary log was created). This value is redundant; the same value occurs in the timestamp header field.
     *
     */
    private long timestamp;
    private int headerLength; // uint<1> The header length. This length - 19 gives the size of the extra headers field at the end of the header for other events.

    /** uint<1> Checksum Algorithm Type */
    private int checksumType;

    // uint<4> CRC32 4 bytes (value matters only if checksum algo is CRC32)

    public FormatDescriptionEvent(int logFormatVersion, String serverVersion, long timestamp, int headerLength, int checksumType) {
        this.logFormatVersion = logFormatVersion;
        this.serverVersion = serverVersion;
        this.timestamp = timestamp;
        this.headerLength = headerLength;
        this.checksumType = checksumType;
    }

    public int getLogFormatVersion() {
        return logFormatVersion;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getHeaderLength() {
        return headerLength;
    }

    public int getChecksumType() {
        return checksumType;
    }

    @Override
    public String toString() {
        return "FormatDescriptionEvent{" +
                "logFormatVersion=" + logFormatVersion +
                ", serverVersion='" + serverVersion + '\'' +
                ", timestamp=" + timestamp +
                ", headerLength=" + headerLength +
                ", checksumType=" + checksumType +
                '}';
    }
}
