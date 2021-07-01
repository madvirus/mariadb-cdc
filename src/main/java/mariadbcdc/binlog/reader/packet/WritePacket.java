package mariadbcdc.binlog.reader.packet;

import mariadbcdc.binlog.reader.io.ByteWriter;

public interface WritePacket {
    void writeTo(ByteWriter writer);
}
