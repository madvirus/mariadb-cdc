package mariadbcdc.binlog.reader.packet.binlog.data;

public interface RowsEvent {
    long getTableId();
}
