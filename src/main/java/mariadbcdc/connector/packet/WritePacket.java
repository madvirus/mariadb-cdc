package mariadbcdc.connector.packet;

import mariadbcdc.connector.io.ByteWriter;

public interface WritePacket {
    void writeTo(ByteWriter writer);
}
