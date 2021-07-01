package mariadbcdc.binlog.packet.binlog.data;

import mariadbcdc.binlog.packet.binlog.BinLogData;

public class XidEvent implements BinLogData {
    private long xid;

    public XidEvent(long xid) {
        this.xid = xid;
    }

    public long getXid() {
        return xid;
    }

    @Override
    public String toString() {
        return "XidEvent{" +
                "xid=" + xid +
                '}';
    }
}
