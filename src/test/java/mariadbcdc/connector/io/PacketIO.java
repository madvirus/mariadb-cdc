package mariadbcdc.connector.io;

import mariadbcdc.connector.BinLogBadPacketException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PacketIO {
    private final InputStream is;
    private final OutputStream os;

    private byte[] readBody = new byte[16777215];
    private int remainingBlock;

    public PacketIO(InputStream is, OutputStream os) {
        this.is = is;
        this.os = os;
    }

    private int read() {
        try {
            int read = is.read();
            if (read == -1) {
                throw new BinLogEOFException("EOF");
            }
            remainingBlock--;
            return read;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public int readInt(int len) {
        try {
            int value = 0;
            for (int i = 0; i < len; i++) {
                value += is.read() << (i * 8);
            }
            remainingBlock -= len;
            return value;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public long readLong(int len) {
        try {
            long value = 0;
            for (int i = 0; i < len; i++) {
                int read = is.read();
                if (read == -1) {
                    throw new BinLogEOFException("EOF");
                }
                value += ((long) read << (i * 8));
            }
            remainingBlock -= len;
            return value;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public String readString(int len) {
        try {
            int read = is.read(readBody, 0, len);
            if (read == -1) {
                throw new BinLogEOFException("-1");
            }
            remainingBlock -= len;
            return new String(readBody, 0, len);
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
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

    public String readLengthEncodedString() {
        int length = readLengthEncoded();
        if (length == -1) return null;
        return readString(length);
    }

    private int readLengthEncoded() {
        int first = read();
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
            // TODO 검토 필요 return readInt(8);
            throw new UnsupportedLengthException(8);
        }
        throw new BinLogBadPacketException("invalid length encoded: " + Integer.toHexString(first));
    }

    public void skip(int length) {
        try {
            is.skip(length);
            remainingBlock -= length;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public void startBlock(int blockLength) {
        this.remainingBlock = blockLength;
    }

    public String readStringNul() {
        try {
            int end = 0;
            while (true) {
                int b = is.read();
                if (b == -1) {
                    throw new BinLogEOFException("EOF");
                }
                remainingBlock--;
                if (b == 0) {
                    break;
                }
                readBody[end] = (byte)b;
                end++;
            }
            String s = new String(readBody, 0, end);
            return s;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public String readStringEOF() {
        return readString(remainingBlock);
    }

    public void skipRemaining() {
        skip(remainingBlock);
    }

    public byte[] readBytes(byte[] buff, int offset, int len) {
        try {
            int read = is.read(buff, offset, len);
            if (read == -1) {
                throw new BinLogEOFException("-1");
            }
            remainingBlock -= len;
            return buff;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public int remainingBlock() {
        return remainingBlock;
    }

    public void writeInt(int value, int len) {
        try {
            int v = value;
            for (int i = 0; i < len; i++) {
                os.write((byte) (v & 0xFF));
                v = v >> 8;
            }
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public void writeBytes(byte[] bytes, int offset, int len) {
        try {
            os.write(bytes, offset, len);
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public void writeByte(byte b) {
        try {
            os.write(b);
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public void flush() {
        try {
            os.flush();
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }
}
