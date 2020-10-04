package mariadbcdc;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

class ColumnNameCacheTest {

    @Test
    void colNames() {
        ColumnNamesGetter mockColNamesGetter = mock(ColumnNamesGetter.class);
        given(mockColNamesGetter.getColumnNames("db", "table")).willReturn(Arrays.asList("col1", "col2", "col3"));

        ColumnNameCache cache = new ColumnNameCache(mockColNamesGetter);

        List<List<String>> colNamesList1 = IntStream.range(0, 10)
                .mapToObj(i -> cache.getColumnNames("db", "table"))
                .collect(Collectors.toList());

        cache.invalidate("db", "table");

        List<List<String>> colNamesList2 = IntStream.range(0, 10)
                .mapToObj(i -> cache.getColumnNames("db", "table"))
                .collect(Collectors.toList());

        SoftAssertions.assertSoftly(soft -> {
            colNamesList1.forEach(colNames -> soft.assertThat(colNames).contains("col1", "col2", "col3"));
            colNamesList2.forEach(colNames -> soft.assertThat(colNames).contains("col1", "col2", "col3"));
            then(mockColNamesGetter).should(times(2)).getColumnNames("db", "table");
        });
    }

}