package mariadbcdc.binlog.reader;

public interface BinLogLifecycleListener {
    BinLogLifecycleListener NULL = new BinLogLifecycleListener.NullBinLogLifecycleListener();

    default void onConnected() {}
    default void onStarted() {}
    default void onDisconnected() {}

    class NullBinLogLifecycleListener implements BinLogLifecycleListener {
    }
}
