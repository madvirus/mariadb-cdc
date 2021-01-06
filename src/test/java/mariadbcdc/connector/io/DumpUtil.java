package mariadbcdc.connector.io;

public class DumpUtil {
    private static char[] hex = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static void toHex(StringBuilder sb, byte b) {
        sb.append(hex[(b >> 4) & 0xF]);
        sb.append(hex[b & 0xF]);
    }

    public static char toChar(byte b) {
        int ch = Byte.toUnsignedInt(b);
        if (Character.isLetterOrDigit(ch)) return (char)ch;
        return '.';
    }

    public static void dumpHex(StringBuilder sb, byte[] bytes, int offset, int len) {
        sb.append(" 0  1  2  3  4  5  6  7   8  9  A  B  C  D  E  F |01234567 89ABCDEF|").append("\n");
        sb.append("----------------------- ------------------------ |-------- --------|").append("\n");
        for (int i = offset ; i < len ; i += 16) {
            int endIdx = i + 16;
            if (endIdx > len) endIdx = len;
            for (int idx = i ; idx < endIdx ; idx ++) {
                DumpUtil.toHex(sb, bytes[idx]);
                sb.append(" ");
                if (idx % 16 == 7) sb.append(" ");
            }
            if (endIdx - i < 16) {
                for (int sp = 0 ; sp < 16 - (endIdx - i) ; sp ++) {
                    sb.append("   ");
                    if (sp == 8) sb.append(" ");
                }
            }
            sb.append("|");
            for (int idx = i ; idx < endIdx ; idx ++) {
                sb.append(DumpUtil.toChar(bytes[idx]));
                if (idx % 16 == 7) sb.append(" ");
            }
            if (endIdx - i < 16) {
                for (int sp = 0 ; sp < (16 - (endIdx - i)) ; sp ++) {
                    sb.append(" ");
                    if (sp == 8) sb.append(" ");
                }
            }
            sb.append("|\n");
        }
    }
}

