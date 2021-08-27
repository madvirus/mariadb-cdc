package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import mariadbcdc.BaseDataRow;
import mariadbcdc.DataRow;
import mariadbcdc.UnsupportedTypeException;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataRowImpl extends BaseDataRow {

    public DataRowImpl() {
    }

    public DataRowImpl add(String colName, ColumnType columnType, Serializable value) {
        if (value == null) {
            addValue(colName, value);
        } else {
            switch (columnType) {
                case DATETIME:
                case DATETIME_V2:
                case TIMESTAMP:
                case TIMESTAMP_V2:
                    if (value instanceof Long) {
                        long timestamp = ((Long) value).longValue();
                        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(timestamp / 1000, (int) (timestamp % 1000) * 1000000, ZoneOffset.UTC);
                        addValue(colName, localDateTime);
                    } else if (value instanceof Timestamp) {
                        addValue(colName, ((Timestamp) value).toLocalDateTime());
                    } else if (value instanceof java.util.Date) {
                        addValue(colName, new Timestamp(((java.util.Date) value).getTime()).toLocalDateTime());
                    } else {
                        addValue(colName, value);
                    }
                    break;
                case DATE:
                    if (value instanceof Long) {
                        LocalDate localDate = new java.sql.Date(((Number) value).longValue()).toLocalDate();
                        addValue(colName, localDate);
                    } else if (value instanceof java.sql.Date) {
                        addValue(colName, ((Date) value).toLocalDate());
                    } else {
                        addValue(colName, value);
                    }
                    break;
                case TIME:
                case TIME_V2:
                    if (value instanceof Long) {
                        LocalTime localTime = new java.sql.Time(((Number) value).longValue()).toLocalTime();
                        addValue(colName, localTime);
                    } else if (value instanceof java.sql.Time) {
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
