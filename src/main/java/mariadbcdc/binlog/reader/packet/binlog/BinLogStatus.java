package mariadbcdc.binlog.reader.packet.binlog;

public class BinLogStatus {
    private final int length;
    private final int seq;
    private final int status;

    public BinLogStatus(int length, int seq, int status) {
        this.length = length;
        this.seq = seq;
        this.status = status;
    }

    public int getLength() {
        return length;
    }

    public int getSeq() {
        return seq;
    }

    public int getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "BinLogStatus{" +
                "length=" + length +
                ", seq=" + seq +
                ", status=" + Integer.toHexString(status) +
                "(" + getStatusStr() + ")" +
                '}';
    }

    private String getStatusStr() {
        switch (status) {
            case 0x00: return "OK";
            case 0xFF: return "ERR";
            case 0xFE: return "EOF";
        }
        return "UNKNOWN";
    }
}
