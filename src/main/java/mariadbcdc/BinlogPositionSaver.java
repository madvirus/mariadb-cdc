package mariadbcdc;

public interface BinlogPositionSaver {
    void save(BinlogPosition binlogPosition);
}
