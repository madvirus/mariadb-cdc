package mariadbcdc.binlog.reader.packet;

public class UnknownReadPacket implements ReadPacket {
    public static final UnknownReadPacket INSTANCE = new UnknownReadPacket();
}
