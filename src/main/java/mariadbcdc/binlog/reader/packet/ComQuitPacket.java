package mariadbcdc.binlog.reader.packet;

import mariadbcdc.binlog.reader.io.ByteWriter;

public class ComQuitPacket implements WritePacket {
    public static final ComQuitPacket INSTANCE = new ComQuitPacket();

    @Override
    public void writeTo(ByteWriter writer) {
        writer.sequenceNumber(0);
        writer.write(0x01, 1);
    }
}
