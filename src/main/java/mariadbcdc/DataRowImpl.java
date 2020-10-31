package mariadbcdc;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataRowImpl implements DataRow {
    private List<Serializable> values = new ArrayList<>();
    private Map<String, Serializable> valueMap = new HashMap<>();
    private boolean hasTableColumnNames;

    @Override
    public String getString(String field) {
        Serializable value = valueMap.get(field.toLowerCase());
        return getStringInternal(value);
    }

    @Override
    public String getString(int index) {
        Serializable value = values.get(index);
        return getStringInternal(value);
    }

    private String getStringInternal(Serializable value) {
        if (value == null) return null;
        if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    @Override
    public Long getLong(String field) {
        Serializable value = valueMap.get(field.toLowerCase());
        return getLongInternal(value);
    }

    @Override
    public Long getLong(int index) {
        Serializable value = values.get(index);
        return getLongInternal(value);
    }

    private Long getLongInternal(Serializable value) {
        if (value == null) return null;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Number) return ((Number) value).longValue();
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public Integer getInt(String field) {
        Serializable value = valueMap.get(field.toLowerCase());
        return getIntInternal(value);
    }

    @Override
    public Integer getInt(int index) {
        Serializable value = values.get(index);
        return getIntInternal(value);
    }

    private Integer getIntInternal(Serializable value) {
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Number) return ((Number) value).intValue();
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public LocalDateTime getLocalDateTime(String field) {
        Serializable value = valueMap.get(field.toLowerCase());
        return getLocalDateTimeInternal(value);
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        Serializable value = values.get(index);
        return getLocalDateTimeInternal(value);
    }

    private LocalDateTime getLocalDateTimeInternal(Serializable value) {
        if (value == null) return null;
        if (value instanceof LocalDateTime) return (LocalDateTime) value;
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public LocalDate getLocalDate(String field) {
        Serializable value = valueMap.get(field.toLowerCase());
        return getLocalDateInternal(value);
    }

    @Override
    public LocalDate getLocalDate(int index) {
        Serializable value = values.get(index);
        return getLocalDateInternal(value);
    }

    private LocalDate getLocalDateInternal(Serializable value) {
        if (value == null) return null;
        if (value instanceof LocalDate) return (LocalDate) value;
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public LocalTime getLocalTime(String field) {
        Serializable value = valueMap.get(field.toLowerCase());
        return getLocalTimeInternal(value);
    }

    @Override
    public LocalTime getLocalTime(int index) {
        Serializable value = values.get(index);
        return getLocalTimeInternal(value);
    }

    private LocalTime getLocalTimeInternal(Serializable value) {
        if (value == null) return null;
        if (value instanceof LocalTime) return (LocalTime) value;
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public Boolean getBoolean(String field) {
        Serializable value = valueMap.get(field.toLowerCase());
        return getBooleanInternal(value);
    }

    @Override
    public Boolean getBoolean(int index) {
        Serializable value = values.get(index);
        return getBooleanInternal(value);
    }

    private Boolean getBooleanInternal(Serializable value) {
        if (value == null) return null;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof Number) return ((Number) value).intValue() == 1;
        if (value instanceof String) return Boolean.valueOf((String) value);
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public boolean hasTableColumnNames() {
        return hasTableColumnNames;
    }

    @Override
    public int getColumnCount() {
        return valueMap.size();
    }

    public DataRowImpl add(String colName, ColumnType columnType, Serializable value) {
        if (value == null) {
            addValue(colName, value);
        }
        else {
            switch (columnType) {
                case DATETIME:
                case DATETIME_V2:
                case TIMESTAMP:
                case TIMESTAMP_V2:
                    if (value instanceof Long) {
                        LocalDateTime localDateTime = new Timestamp(((Number) value).longValue()).toLocalDateTime();
                        addValue(colName, localDateTime);
                    } else if (value instanceof Timestamp) {
                        addValue(colName, ((Timestamp) value).toLocalDateTime());
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

    private Serializable addValue(String colName, Serializable value) {
        values.add(value);
        return valueMap.put(colName.toLowerCase(), value);
    }

    public void setHasTableColumnNames(boolean hasTableColumnNames) {
        this.hasTableColumnNames = hasTableColumnNames;
    }
}
