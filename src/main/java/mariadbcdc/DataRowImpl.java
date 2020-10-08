package mariadbcdc;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

public class DataRowImpl implements DataRow {
    private Map<String, Serializable> values = new HashMap<>();
    private boolean hasTableColumnNames;

    @Override
    public String getString(String field) {
        Serializable value = values.get(field.toLowerCase());
        if (value == null) return null;
        if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    @Override
    public Long getLong(String field) {
        Serializable value = values.get(field.toLowerCase());
        if (value == null) return null;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Number) return ((Number) value).longValue();
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new UnsupportedTypeException("not supported type: " + field.getClass().getName());
    }

    @Override
    public Integer getInt(String field) {
        Serializable value = values.get(field.toLowerCase());
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Number) return ((Number) value).intValue();
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new UnsupportedTypeException("not supported type: " + field.getClass().getName());
    }

    @Override
    public LocalDateTime getLocalDateTime(String field) {
        Serializable value = values.get(field.toLowerCase());
        if (value == null) return null;
        if (value instanceof LocalDateTime) return (LocalDateTime) value;
        throw new UnsupportedTypeException("not supported type: " + field.getClass().getName());
    }

    @Override
    public LocalDate getLocalDate(String field) {
        Serializable value = values.get(field.toLowerCase());
        if (value == null) return null;
        if (value instanceof LocalDate) return (LocalDate) value;
        throw new UnsupportedTypeException("not supported type: " + field.getClass().getName());
    }

    @Override
    public LocalTime getLocalTime(String field) {
        Serializable value = values.get(field.toLowerCase());
        if (value == null) return null;
        if (value instanceof LocalTime) return (LocalTime) value;
        throw new UnsupportedTypeException("not supported type: " + field.getClass().getName());
    }

    @Override
    public Boolean getBoolean(String field) {
        Serializable value = values.get(field.toLowerCase());
        if (value == null) return null;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof Number) return ((Number)value).intValue() == 1;
        if (value instanceof String) return Boolean.valueOf((String)value);
        throw new UnsupportedTypeException("not supported type: " + field.getClass().getName());
    }

    @Override
    public boolean hasTableColumnNames() {
        return hasTableColumnNames;
    }

    @Override
    public int getColumnCount() {
        return values.size();
    }

    public DataRowImpl add(String colName, ColumnType columnType, Serializable value) {
        if (value == null) values.put(colName.toLowerCase(), value);
        else {
            switch (columnType) {
                case DATETIME:
                case DATETIME_V2:
                case TIMESTAMP:
                case TIMESTAMP_V2:
                    if (value instanceof Long) {
                        LocalDateTime localDateTime = new Timestamp(((Number) value).longValue()).toLocalDateTime();
                        values.put(colName.toLowerCase(), localDateTime);
                    } else if (value instanceof Timestamp) {
                        values.put(colName.toLowerCase(), ((Timestamp) value).toLocalDateTime());
                    } else {
                        values.put(colName.toLowerCase(), value);
                    }
                    break;
                case DATE:
                    if (value instanceof Long) {
                        LocalDate localDate = new java.sql.Date(((Number) value).longValue()).toLocalDate();
                        values.put(colName.toLowerCase(), localDate);
                    } else if (value instanceof java.sql.Date) {
                        values.put(colName.toLowerCase(), ((java.sql.Date) value).toLocalDate());
                    } else {
                        values.put(colName.toLowerCase(), value);
                    }
                    break;
                case TIME:
                case TIME_V2:
                    if (value instanceof Long) {
                        LocalTime localTime = new java.sql.Time(((Number) value).longValue()).toLocalTime();
                        values.put(colName.toLowerCase(), localTime);
                    } else if (value instanceof java.sql.Time) {
                        values.put(colName.toLowerCase(), ((java.sql.Time) value).toLocalTime());
                    } else {
                        values.put(colName.toLowerCase(), value);
                    }
                    break;
                default:
                    values.put(colName.toLowerCase(), value);
            }
        }
        return this;
    }

    public void setHasTableColumnNames(boolean hasTableColumnNames) {
        this.hasTableColumnNames = hasTableColumnNames;
    }
}
