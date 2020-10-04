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

    default void onXid(Long xid) {
    }

    default void stopped() {
    }

    class BaseListener implements MariadbCdcListener {
    }

}
