package mariadbcdc.binlog;

import mariadbcdc.BaseDataRow;
import mariadbcdc.binlog.reader.FieldType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

public class BinLogReaderDataRow extends BaseDataRow {

    public BinLogReaderDataRow() {
    }

    public BinLogReaderDataRow add(String colName, FieldType columnType, Object value) {
        if (value == null) {
            addValue(colName, value);
        } else {
            switch (columnType) {
                case DATETIME:
                case DATETIME2:
                case TIMESTAMP:
                case TIMESTAMP2:
                    if (value instanceof Long) {
                        long timestamp = ((Long) value).longValue();
                        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(timestamp / 1000, (int) (timestamp % 1000) * 1000000, ZoneOffset.UTC);
                        addValue(colName, localDateTime);
                    } else if (value instanceof Timestamp) {
                        addValue(colName, ((Timestamp) value).toLocalDateTime());
                    } else {
                        addValue(colName, value);
                    }
                    break;
                case DATE:
                    if (value instanceof Long) {
                        LocalDate localDate = new Date(((Number) value).longValue()).toLocalDate();
                        addValue(colName, localDate);
                    } else if (value instanceof Date) {
                        addValue(colName, ((Date) value).toLocalDate());
                    } else {
                        addValue(colName, value);
                    }
                    break;
                case TIME:
                case TIME2:
                    if (value instanceof Long) {
                        LocalTime localTime = new Time(((Number) value).longValue()).toLocalTime();
                        addValue(colName, localTime);
                    } else if (value instanceof Time) {
                        addValue(colName, ((Time) value).toLocalTime());
                    } else {
                        addValue(colName, value);
                    }
                    break;
                default:
                    addValue(colName, value);
            }
        }
        return this;
    }
}
