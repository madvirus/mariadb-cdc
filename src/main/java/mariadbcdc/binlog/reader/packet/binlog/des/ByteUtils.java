package mariadbcdc.binlog.reader.packet.binlog.des;

public class ByteUtils {

    public static int toBigEndianInt(byte[] bytes, int off, int len) {
        int value = 0;
        for (int i = 0 ; i < len ; i++) {
            value = (value << 8) | Byte.toUnsignedInt(bytes[off + i]);
        }
        return value;
    }

    public static int toLittleEndianInt(byte[] bytes, int off, int len) {
        int value = 0;
        for (int i = 0; i < len; i++) {
            value |= Byte.toUnsignedInt(bytes[off + i]) << (i * 8);
        }
        return value;
    }
}
