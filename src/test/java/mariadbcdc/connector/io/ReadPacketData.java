package mariadbcdc.connector.io;

import mariadbcdc.connector.BinLogBadPacketException;

public class ReadPacketData {

    private int packetLength;
    private int sequenceNumber;
    private byte[] bytes;
    private int idx = 0;

    public ReadPacketData(int packetLength, int sequenceNumber, byte[] bytes) {
        this.packetLength = packetLength;
        this.sequenceNumber = sequenceNumber;
        this.bytes = bytes;
    }

    public int getPacketLength() {
        return packetLength;
    }

    public int getRealPacketLength() {
        return packetLength + 4;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    private byte get() {
        if (idx == packetLength) {
            throw new BinLogEOFException(String.format("get(): idx=%d, packetLength=%d", idx, packetLength));
        }
        return bytes[idx++];
    }

    public int readInt(int len) {
        if (len > 4) throw new UnsupportedLengthException(len);
        int value = 0;
        for (int i = 0; i < len; i++) {
            value += (get() & 0xFF) << (i * 8);
        }
        return value;
    }

    public long readLong(int len) {
        long value = 0;
        for (int i = 0; i < len; i++) {
            value += ((long) (get() & 0xFF)) << (i * 8);
        }
        return value;
    }

    private String getString(int offset, int len) {
        if (offset + len > packetLength) {
            throw new BinLogEOFException(
                    String.format("getString(%d, %d), packetLength: %d",
                            offset, len, packetLength)
            );
        }
        String s = new String(bytes, offset, len);
        idx += len;
        return s;
    }

    public String readString(int len) {
        return getString(idx, len);
    }

    public String readStringNul() {
        int start = idx;
        int end = idx;
        while (true) {
            if (end == packetLength) {
                throw new BinLogEOFException(
                        String.format("readStringNul(): end=%d, packetLength=%d", end, packetLength));
            }
            if (bytes[end] == 0x00) {
                break;
            }
            end++;
        }
        String s = getString(start, end - start);
        get(); // nul 다음으로 이동 처리
        return s;
    }

    public String readLengthEncodedString() {
        int length = readLengthEncoded();
        if (length == -1) return null;
        return readString(length);
    }

    private int readLengthEncoded() {
        int first = Byte.toUnsignedInt(get());
        if (first < 0xFB)
            return first;
        if (first == 0xFB) return -1;
        if (first == 0xFC) {
            return readInt(2);
        }
        if (first == 0xFD) {
            return readInt(3);
        }
        if (first == 0xFE) {
            // return readInt(8);
            throw new UnsupportedLengthException(8);
        }
        throw new BinLogBadPacketException("invalid length encoded: " + Integer.toHexString(first));
    }

    public int getFirstByte() {
        return Byte.toUnsignedInt(bytes[0]);
    }

    public int readLengthEncodedInt() {
        int length = readLengthEncoded();
        if (length < 0xFB) {
            return length;
        }
        return readInt(length);
    }

    public long readLengthEncodedLong() {
        int length = readLengthEncoded();
        if (length < 0xFB) {
            return length;
        }
        return readLong(length);
    }

    public String readStringEOF() {
        return readString(packetLength - idx);
    }

    public void rewind(int i) {
        idx -= i;
    }

    public byte[] readBytesEof() {
        return readBytes(packetLength - idx - 1);
    }

    public byte[] readBytes(int len) {
        return getBytes(idx, len);
    }

    private byte[] getBytes(int offset, int len) {
        if (offset + len > packetLength) {
            throw new BinLogEOFException(
                    String.format("getBytes(%d, %d), packetLength: %d",
                            offset, len, packetLength)
            );
        }
        byte[] result = new byte[len];
        System.arraycopy(bytes, offset, result, 0, len);
        idx += len;
        return result;
    }

    public String toString() {
        return new String(bytes, 0, packetLength);
    }

    public void dump(StringBuilder sb) {
        sb.append("packet length: ").append(getPacketLength())
                .append(", sequence number: ").append(getSequenceNumber())
                .append("\n");

        DumpUtil.dumpHex(sb, bytes, 0, packetLength);
    }

    public byte[] getRawBodyBytes() {
        return bytes;
    }
}
