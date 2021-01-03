package mariadbcdc.connector.io;

import java.io.IOException;
import java.io.OutputStream;

public class WritePacketData {
    private int sequenceNumber;
    private byte[] buff;
    private int packetLength;

    public WritePacketData(int sequenceNumber, byte[] buff, int packetLength) {
        this.sequenceNumber = sequenceNumber;
        this.buff = buff;
        this.packetLength = packetLength;
    }

    public void send(OutputStream out) {
        try {
            writePacketLength(out);
            out.write((byte)sequenceNumber);
            out.write(buff, 0, packetLength);
            out.flush();
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    private void writePacketLength(OutputStream out) throws IOException {
        int len = packetLength;
        for (int i = 0; i < 3; i++) {
            out.write((byte) (len & 0xFF));
            len = len >> 8;
        }
    }

    public void dump(StringBuilder sb) {
        sb.append("packet length: ").append(packetLength)
                .append(", sequence number: ").append(sequenceNumber)
                .append("\n");

        DumpUtil.dumpHex(sb, buff, 0, packetLength);
    }
}
