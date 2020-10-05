package mariadbcdc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public interface DataRow {
    String getString(String field);

    Long getLong(String field);

    Integer getInt(String field);

    LocalDateTime getLocalDateTime(String field);

    LocalDate getLocalDate(String field);

    LocalTime getLocalTime(String field);

    Boolean getBoolean(String field);

    boolean hasTableColumnNames();

    int getColumnCount();
}
