package mariadbcdc.binlog.reader.io;

public class BufferByteWriter implements ByteWriter {
    private int idx = 0;
    private byte[] buff;
    private int sequenceNumber;

    public BufferByteWriter(byte[] buff) {
        this.buff = buff;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public int getPacketLength() {
        return idx;
    }

    private void put(byte b) {
        buff[idx] = b;
        idx++;
    }

    private void put(byte[] ba) {
        System.arraycopy(ba, 0, buff, idx, ba.length);
        idx += ba.length;
    }

    @Override
    public void sequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public void write(int value, int len) {
        for (int i = 0; i < len; i++) {
            put((byte) (value & 0xFF));
            value = value >>> 8;
        }
    }

    @Override
    public void write(long value, int len) {
        for (int i = 0; i < len; i++) {
            put((byte) (value & 0xFF));
            value = value >>> 8;
        }
    }

    @Override
    public void writeBytes(byte[] bytes) {
        put(bytes);
    }

    @Override
    public void writeBytesLenenc(byte[] bytes) {
        writeEncodedLength(bytes.length);
        put(bytes);
    }

    @Override
    public void writeEncodedLength(long length) {
        if (length < 0xFB) {
            put((byte) length);
            return;
        }
        if (length < 65536) {
            put((byte) 0xfc);
            put((byte) length);
            put((byte) (length >>> 8));
            return;
        }

        if (length < 16777216) {
            put((byte) 0xfd);
            put((byte) length);
            put((byte) (length >>> 8));
            put((byte) (length >>> 16));
            return;
        }
        throw new UnsupportedLengthException(length);
    }

    @Override
    public void writeStringLenenc(String str) {
        if (str == null) {
            put((byte) 0xFB);
        } else if (str.isEmpty()) {
            put((byte)0x00);
        } else {
            writeBytesLenenc(str.getBytes());
        }
    }

    @Override
    public void writeStringNul(String str) {
        put(str.getBytes());
        put((byte) 0);
    }

    @Override
    public void writeString(String str) {
        put(str.getBytes());
    }

    @Override
    public void writeZero() {
        put((byte) 0);
    }

    @Override
    public void reserved(int len) {
        for (int i = 0; i < len; i++) {
            put((byte) 0);
        }
    }

}
