package mariadbcdc.binlog.packet.binlog.data;

public class RowsPair {
    public final Object[] before;
    public final Object[] after;

    public RowsPair(Object[] before, Object[] after) {
        this.before = before;
        this.after = after;
    }

    public Object[] getBefore() {
        return before;
    }

    public Object[] getAfter() {
        return after;
    }
}
