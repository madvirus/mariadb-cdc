package mariadbcdc.binlog.io;

public interface ByteWriter {
    void sequenceNumber(int sequenceNumber);

    void write(int value, int len);

    void write(long value, int len);

    void writeBytes(byte[] bytes);

    void writeBytesLenenc(byte[] bytes);

    void writeEncodedLength(long length);

    void writeStringLenenc(String str);

    void writeStringNul(String str);

    void writeString(String str);

    void writeZero();

    void reserved(int len);
}
