package mariadbcdc;

import java.util.List;

public interface MariadbCdcListener {
    MariadbCdcListener NO_OP = new BaseListener();

    default void started(BinlogPosition binlogPosition) {
    }

    default void startFailed(Exception e) {
    }

    default void onDataChanged(List<RowChangedData> list) {
    }

    default void onXid(BinlogPosition nextPosition, Long xid) {
        onXid(xid);
    }

    default void onXid(Long xid) {
    }

    default void stopped() {
    }

    class BaseListener implements MariadbCdcListener {
    }

}
