package mariadbcdc.connector;

import mariadbcdc.connector.packet.ErrPacket;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.data.*;

public interface BinLogListener {
    BinLogListener NULL = new NullBinLogListener();

    default void onErr(ErrPacket err) {}

    default void onRotateEvent(BinLogHeader header, RotateEvent data) {}

    default void onFormatDescriptionEvent(BinLogHeader header, QueryEvent data) {}

    default void onQueryEvent(BinLogHeader header, QueryEvent data) {}

    default void onTableMapEvent(BinLogHeader header, TableMapEvent data) {}

    default void onWriteRowsEvent(BinLogHeader header, WriteRowsEvent data) {}

    default void onUpdateRowsEvent(BinLogHeader header, UpdateRowsEvent data) {}

    default void onDeleteRowsEvent(BinLogHeader header, DeleteRowsEvent data) {}

    default void onXidEvent(BinLogHeader header, XidEvent data) {}

    default void onHeartbeatEvent(BinLogHeader header, HeartbeatEvent data) {}

    default void onStopEvent(BinLogHeader header, StopEvent data) {}

    class NullBinLogListener implements BinLogListener {
    }
}
