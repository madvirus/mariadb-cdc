package mariadbcdc.binlog.reader.packet.query;

import mariadbcdc.binlog.reader.io.ByteWriter;
import mariadbcdc.binlog.reader.packet.WritePacket;

public class ComQueryPacket implements WritePacket {
    private int header = 0x03;
    private String sql;

    public ComQueryPacket(String sql) {
        this.sql = sql;
    }

    @Override
    public void writeTo(ByteWriter writer) {
        writer.sequenceNumber(0);
        writer.write(header, 1);
        writer.writeString(sql);
    }

    @Override
    public String toString() {
        return "ComQueryPacket{" +
                "header=" + header +
                ", sql='" + sql + '\'' +
                '}';
    }
}
