package mariadbcdc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

public interface DataRow {
    String getString(String field);
    String getString(int index);

    Long getLong(String field);
    Long getLong(int index);

    Integer getInt(String field);
    Integer getInt(int index);

    LocalDateTime getLocalDateTime(String field);
    LocalDateTime getLocalDateTime(int index);

    LocalDate getLocalDate(String field);
    LocalDate getLocalDate(int index);

    LocalTime getLocalTime(String field);
    LocalTime getLocalTime(int index);

    Boolean getBoolean(String field);
    Boolean getBoolean(int index);

    boolean hasTableColumnNames();

    int getColumnCount();
    List<String> getColumnNames();
}
