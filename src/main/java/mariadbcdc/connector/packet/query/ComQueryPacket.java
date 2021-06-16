package mariadbcdc.connector.packet.query;

import mariadbcdc.connector.io.ByteWriter;
import mariadbcdc.connector.packet.WritePacket;

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
