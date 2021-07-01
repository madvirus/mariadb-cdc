package mariadbcdc.binlog.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PacketIO {
    private final InputStream is;
    private final OutputStream os;

    private byte[] readBody = new byte[16777215];

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
            return read;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public int readInt(int len) {
        int value = 0;
        for (int i = 0; i < len; i++) {
            value += read() << (i * 8);
        }
        return value;
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
            return value;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
    }

    public byte[] readBytes(byte[] buff, int offset, int len) {
        try {
            int remaining = len;
            while (remaining != 0) {
                int readCnt = is.read(buff, offset + len - remaining, remaining);
                if (readCnt == -1) {
                    throw new EOFException();
                }
                remaining -= readCnt;
            }
            return buff;
        } catch (IOException e) {
            throw new BinLogIOException(e);
        }
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
