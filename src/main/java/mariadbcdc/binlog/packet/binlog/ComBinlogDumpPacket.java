package mariadbcdc.binlog.packet.binlog;

import mariadbcdc.binlog.io.ByteWriter;
import mariadbcdc.binlog.packet.WritePacket;

public class ComBinlogDumpPacket implements WritePacket {
    private int command = 0x12;
    private long binlogPosition;
    private int flags;
    private long slaveServerId;
    private String binlogFilename;

    public ComBinlogDumpPacket(long binlogPosition, int flags, long slaveServerId, String binlogFilename) {
        this.binlogPosition = binlogPosition;
        this.flags = flags;
        this.slaveServerId = slaveServerId;
        this.binlogFilename = binlogFilename;
    }

    @Override
    public void writeTo(ByteWriter writer) {
        writer.write(command, 1);
        writer.write(binlogPosition, 4);
        writer.write(0, 2);
        writer.write(slaveServerId, 4);
        writer.writeString(binlogFilename);
    }

    @Override
    public String toString() {
        return "ComBinlogDumpPacket{" +
                "command=" + command +
                ", binlogPosition=" + binlogPosition +
                ", flags=" + flags +
                ", slaveServerId=" + slaveServerId +
                ", binlogFilename='" + binlogFilename + '\'' +
                '}';
    }
}
