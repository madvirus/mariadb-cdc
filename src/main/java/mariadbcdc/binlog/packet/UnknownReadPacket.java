package mariadbcdc.binlog.packet;

public class UnknownReadPacket implements ReadPacket {
    public static final UnknownReadPacket INSTANCE = new UnknownReadPacket();
}
