package mariadbcdc;

import java.io.Serializable;

public class BeforeAfterRow {
    private Serializable[] before;
    private Serializable[] after;

    public BeforeAfterRow(Serializable[] before, Serializable[] after) {
        this.before = before;
        this.after = after;
    }

    public Serializable[] getBefore() {
        return before;
    }

    public Serializable[] getAfter() {
        return after;
    }
}
