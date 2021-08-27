package mariadbcdc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

public class BaseDataRow implements DataRow {
    private List<Object> values = new ArrayList<>();
    private Map<String, Object> valueMap = new HashMap<>();
    private boolean hasTableColumnNames;
    private List<String> columnNames = new ArrayList<>();

    @Override
    public String getString(String field) {
        Object value = valueMap.get(field.toLowerCase());
        return getStringInternal(value);
    }

    @Override
    public String getString(int index) {
        Object value = values.get(index);
        return getStringInternal(value);
    }

    private String getStringInternal(Object value) {
        if (value == null) return null;
        if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    @Override
    public Long getLong(String field) {
        Object value = valueMap.get(field.toLowerCase());
        return getLongInternal(value);
    }

    @Override
    public Long getLong(int index) {
        Object value = values.get(index);
        return getLongInternal(value);
    }

    private Long getLongInternal(Object value) {
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
        Object value = valueMap.get(field.toLowerCase());
        return getIntInternal(value);
    }

    @Override
    public Integer getInt(int index) {
        Object value = values.get(index);
        return getIntInternal(value);
    }

    private Integer getIntInternal(Object value) {
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
        Object value = valueMap.get(field.toLowerCase());
        return getLocalDateTimeInternal(value);
    }

    @Override
    public LocalDateTime getLocalDateTime(int index) {
        Object value = values.get(index);
        return getLocalDateTimeInternal(value);
    }

    private LocalDateTime getLocalDateTimeInternal(Object value) {
        if (value == null) return null;
        if (value instanceof LocalDateTime) return (LocalDateTime) value;
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public LocalDate getLocalDate(String field) {
        Object value = valueMap.get(field.toLowerCase());
        return getLocalDateInternal(value);
    }

    @Override
    public LocalDate getLocalDate(int index) {
        Object value = values.get(index);
        return getLocalDateInternal(value);
    }

    private LocalDate getLocalDateInternal(Object value) {
        if (value == null) return null;
        if (value instanceof LocalDate) return (LocalDate) value;
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public LocalTime getLocalTime(String field) {
        Object value = valueMap.get(field.toLowerCase());
        return getLocalTimeInternal(value);
    }

    @Override
    public LocalTime getLocalTime(int index) {
        Object value = values.get(index);
        return getLocalTimeInternal(value);
    }

    private LocalTime getLocalTimeInternal(Object value) {
        if (value == null) return null;
        if (value instanceof LocalTime) return (LocalTime) value;
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    @Override
    public Boolean getBoolean(String field) {
        Object value = valueMap.get(field.toLowerCase());
        return getBooleanInternal(value);
    }

    @Override
    public Boolean getBoolean(int index) {
        Object value = values.get(index);
        return getBooleanInternal(value);
    }

    private Boolean getBooleanInternal(Object value) {
        if (value == null) return null;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof Number) return ((Number) value).intValue() == 1;
        if (value instanceof String) return Boolean.valueOf((String) value);
        throw new UnsupportedTypeException("not supported type: " + value.getClass().getName());
    }

    public void setHasTableColumnNames(boolean hasTableColumnNames) {
        this.hasTableColumnNames = hasTableColumnNames;
    }

    @Override
    public boolean hasTableColumnNames() {
        return hasTableColumnNames;
    }

    @Override
    public int getColumnCount() {
        return valueMap.size();
    }

    @Override
    public List<String> getColumnNames() {
        return Collections.unmodifiableList(columnNames);
    }

    protected Object addValue(String colName, Object value) {
        columnNames.add(colName);
        values.add(value);
        return valueMap.put(colName.toLowerCase(), value);
    }

}
