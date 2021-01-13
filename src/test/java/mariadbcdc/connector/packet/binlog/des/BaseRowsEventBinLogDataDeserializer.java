package mariadbcdc.connector.packet.binlog.des;

import mariadbcdc.connector.FieldType;
import mariadbcdc.connector.io.ReadPacketData;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;

public abstract class BaseRowsEventBinLogDataDeserializer implements BinLogDataDeserializer {

    protected Object[] readColumnValues(BitSet columnUsed, BitSet nullBitmap, TableMapEvent tableMapEvent, ReadPacketData packetIO) {
        int[] metadata = tableMapEvent.getMetadata();
        int colNum = tableMapEvent.getNumberOfColumns();
        FieldType[] fieldTypes = tableMapEvent.getFieldTypes();
        Object[] values = new Object[countOfUsedColumn(colNum, columnUsed)];
        int skipCount = 0;
        for (int i = 0; i < fieldTypes.length; i++) {
            if (!columnUsed.get(i)) {
                skipCount++;
                continue;
            }
            int valIdx = i - skipCount;
            if (!nullBitmap.get(valIdx)) {
                values[valIdx] = readValue(fieldTypes[i], metadata[i], packetIO);
            }
        }
        return values;
    }

    private int countOfUsedColumn(int colNum, BitSet bitSet) {
        int result = 0;
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            result++;
            if (result == colNum) break;
        }
        return result;
    }

    private Object readValue(FieldType fieldType, int metadata, ReadPacketData packetIO) {
        switch (fieldType) {
            case BIT:
                return readBit(metadata, packetIO);
            case TINY:
                return (int) ((byte) packetIO.readInt(1));
            case SHORT:
                return (int) ((short) packetIO.readInt(2));
            case YEAR:
                return packetIO.readInt(2);
            case INT24:
                return (packetIO.readInt(3) << 8) >> 8;
            case LONG:
                return packetIO.readInt(4);
            case LONGLONG:
                return packetIO.readLong(8);
            case FLOAT:
                return Float.intBitsToFloat(packetIO.readInt(4));
            case DOUBLE:
                return Double.longBitsToDouble(packetIO.readInt(8));
            case NEWDECIMAL:
                return readNewDecimal(metadata, packetIO);
            case BLOB:
            case TINY_BLOB:
            case MEDIUM_BLOB:
            case LONG_BLOB:
                return readBlob(metadata, packetIO);
            case STRING:
            case SET:
            case ENUM:
                return readString(metadata, packetIO);
            case VARCHAR:
            case VAR_STRING:
                return readVarchar(metadata, packetIO);
            case DATETIME:
                return readDatetime(packetIO);
            case DATETIME2:
                return readDatetime2(metadata, packetIO);
            case TIME:
                return readTime(packetIO);
            case TIME2:
                return readTime2(metadata, packetIO);
            case DATE:
                return readDate(packetIO);
            case TIMESTAMP:
                return new Timestamp(packetIO.readLong(4) * 1000);
            case TIMESTAMP2:
                return readTimestamp2(metadata, packetIO);
            case GEOMETRY:
                return readGeometry(metadata, packetIO);
            case JSON:
                return readJson(metadata, packetIO);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    private BitSet readBit(int metadata, ReadPacketData packetIO) {
        int bitLen = (metadata >> 8) * 8 + (metadata & 0xFF);
        byte[] bytes = packetIO.readBytes((bitLen + 7) >> 3);
        for (int i = 0; i < bytes.length / 2; i++) {
            byte b1 = bytes[i];
            bytes[i] = bytes[bytes.length - 1 - i];
            bytes[bytes.length - 1 - i] = b1;
        }
        BitSet bitSet = new BitSet();
        for (int i = 0; i < bitLen; i++) {
            if ((bytes[i >> 3] & (1 << (i % 8))) != 0) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    // copied from shyiko mysql binlog
    final int digPerDec = 9;
    final int[] digToBytes = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    private Object readNewDecimal(int metadata, ReadPacketData packetIO) {
        int precision = metadata & 0xFF;
        int scale = metadata >> 8;
        int x = precision - scale;
        int ipd = x / digPerDec;
        int fpd = scale / digPerDec;
        int decimalLength = (ipd << 2) + digToBytes[x - ipd * digPerDec] +
                (fpd << 2) + digToBytes[scale - fpd * digPerDec];

        return asBigDecimal(precision, scale, packetIO.readBytes(decimalLength));
    }

    /**
     * see mysql/strings/decimal.c
     */
    public BigDecimal asBigDecimal(int precision, int scale, byte[] value) {
        boolean positive = (value[0] & 0x80) == 0x80;
        value[0] ^= 0x80;
        if (!positive) {
            for (int i = 0; i < value.length; i++) {
                value[i] ^= 0xFF;
            }
        }
        int x = precision - scale;
        int ipDigits = x / digPerDec;
        int ipDigitsX = x - ipDigits * digPerDec;
        int ipSize = (ipDigits << 2) + digToBytes[ipDigitsX];
        int offset = digToBytes[ipDigitsX];
        BigDecimal ip = offset > 0 ? BigDecimal.valueOf(ByteUtils.toBigEndianInt(value, 0, offset)) : BigDecimal.ZERO;
        for (; offset < ipSize; offset += 4) {
            int i = ByteUtils.toBigEndianInt(value, offset, 4);
            ip = ip.movePointRight(digPerDec).add(BigDecimal.valueOf(i));
        }
        int shift = 0;
        BigDecimal fp = BigDecimal.ZERO;
        for (; shift + digPerDec <= scale; shift += digPerDec, offset += 4) {
            int i = ByteUtils.toBigEndianInt(value, offset, 4);
            fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + digPerDec));
        }
        if (shift < scale) {
            int i = ByteUtils.toBigEndianInt(value, offset, digToBytes[scale - shift]);
            fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
        }
        BigDecimal result = ip.add(fp);
        return positive ? result : result.negate();
    }

    private byte[] readBlob(int metadata, ReadPacketData packetIO) {
        int len = packetIO.readInt(metadata);
        return packetIO.readBytes(len);
    }

    private String readVarchar(int metadata, ReadPacketData packetIO) {
        int len = metadata <= 255 ? packetIO.readInt(1) : packetIO.readInt(2);
        return packetIO.readString(len);
    }

    private String readString(int metadata, ReadPacketData packetIO) {
        int len = metadata & 0xFF;
        return packetIO.readString(len);
    }

    private LocalTime readTime(ReadPacketData packetIO) {
        int value = packetIO.readInt(3);
        int seconds = value % 100;
        value = value / 100;
        int minutes = value % 100;
        value = value / 100;
        int hours = value;
        return LocalTime.of(hours, minutes, seconds);
    }

    private LocalTime readTime2(int metadata, ReadPacketData packetIO) {
        int value = packetIO.readBigEndianInt(3);
        int fsp = readFsp(metadata, packetIO);
        return new Time(value * 1000).toLocalTime().withNano(
                fsp * 1000
        );
    }

    private int readFsp(int metadata, ReadPacketData packetIO) {
        int fractLength = (metadata + 1) / 2;
        int fsp = 0;
        if (fractLength > 0) {
            int fraction = packetIO.readBigEndianInt(fractLength);
            if (fractLength == 3) fsp = fraction * 1;
            if (fractLength == 2) fsp = fraction * 100;
            if (fractLength == 1) fsp = fraction * 10000;
        }
        return fsp;
    }

    private LocalDate readDate(ReadPacketData packetIO) {
        int value = packetIO.readInt(3);
        int days = value & 0b11111; // 1 to 5 bit
        value >>= 5;
        int months = value & 0b1111; // 6 to 9 bit
        value >>= 4;
        int years = value;
        return LocalDate.of(years, months, days);
    }

    private LocalDateTime readDatetime(ReadPacketData packetIO) {
        long value = packetIO.readLong(8);
        int seconds = (int) (value % 100);
        value = value / 100;
        int minutes = (int) (value % 100);
        value = value / 100;
        int hours = (int) (value % 100);
        value = value / 100;
        int days = (int) (value % 100);
        value = value / 100;
        int months = (int) (value % 100);
        value = value / 100;
        int years = (int) value;

        return LocalDateTime.of(years, months, days, hours, minutes, seconds);
    }

    private LocalDateTime readDatetime2(int metadata, ReadPacketData packetIO) {
        // 1 bit  sign           (1= non-negative, 0= negative)
        //17 bits year*13+month  (year 0-9999, month 0-12)
        // 5 bits day            (0-31)
        // 5 bits hour           (0-23)
        // 6 bits minute         (0-59)
        // 6 bits second         (0-59)
        //---------------------------
        //40 bits = 5 bytes
        long value = packetIO.readBigEndianLong(5);
        int second = (int)(value & 0b111111);
        value >>= 6;
        int minute = (int)(value & 0b111111);
        value >>= 6;
        int hour = (int)(value & 0b11111);
        value >>= 5;
        int day = (int)(value & 0b11111);
        value >>= 5;
        int yearMonth = (int)(value & 0x1FFFF);
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int fsp = readFsp(metadata, packetIO);
        return LocalDateTime.of(year, month, day, hour, minute, second, fsp * 1000);
    }

    private Timestamp readTimestamp2(int metadata, ReadPacketData packetIO) {
        return new Timestamp(packetIO.readLong(4) * 1000 + readFsp(metadata, packetIO) / 1000);
    }

    private byte[] readGeometry(int metadata, ReadPacketData packetIO) {
        int len = packetIO.readInt(metadata);
        return packetIO.readBytes(len);
    }

    private byte[] readJson(int metadata, ReadPacketData packetIO) {
        int len = packetIO.readInt(metadata);
        return packetIO.readBytes(len);
    }
}