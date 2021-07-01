package mariadbcdc.binlog.reader.packet.connection;

import mariadbcdc.binlog.reader.MariadbPassword;
import mariadbcdc.binlog.reader.io.ByteWriter;
import mariadbcdc.binlog.reader.packet.WritePacket;

import java.util.Arrays;

public class AuthSwitchResponsePacket implements WritePacket {
    private final int sequenceNumber;
    private final String password;
    private final byte[] seed;

    public AuthSwitchResponsePacket(int sequenceNumber, String password, byte[] seed) {
        this.sequenceNumber = sequenceNumber;
        this.password = password;
        this.seed = seed;
    }

    @Override
    public void writeTo(ByteWriter writer) {
        writer.sequenceNumber(sequenceNumber);
        writer.writeBytes(MariadbPassword.nativePassword(password, seed));
    }

    @Override
    public String toString() {
        return "AuthSwitchResponsePacket{" +
                "sequenceNumber=" + sequenceNumber +
                ", password='" + password + '\'' +
                ", seed=" + Arrays.toString(seed) +
                '}';
    }
}
