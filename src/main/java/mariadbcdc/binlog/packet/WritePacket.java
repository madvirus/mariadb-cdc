package mariadbcdc.binlog.packet;

import mariadbcdc.binlog.io.ByteWriter;

public interface WritePacket {
    void writeTo(ByteWriter writer);
}
