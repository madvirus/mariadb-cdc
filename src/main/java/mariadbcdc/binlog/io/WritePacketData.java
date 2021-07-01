package mariadbcdc.binlog.io;

public class WritePacketData {
    private int sequenceNumber;
    private byte[] buff;
    private int packetLength;

    public WritePacketData(int sequenceNumber, byte[] buff, int packetLength) {
        this.sequenceNumber = sequenceNumber;
        this.buff = buff;
        this.packetLength = packetLength;
    }

    public void send(PacketIO packetIO) {
        packetIO.writeInt(packetLength, 3);
        packetIO.writeByte((byte)sequenceNumber);
        packetIO.writeBytes(buff, 0, packetLength);
        packetIO.flush();
    }

    public void dump(StringBuilder sb) {
        sb.append("packet length: ").append(packetLength)
                .append(", sequence number: ").append(sequenceNumber)
                .append("\n");

        DumpUtil.dumpHex(sb, buff, 0, packetLength);
    }
}
