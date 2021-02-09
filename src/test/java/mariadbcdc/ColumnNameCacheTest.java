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
    void cached() {
        ColumnNamesGetter mockColNamesGetter = mock(ColumnNamesGetter.class);
        given(mockColNamesGetter.getColumnNames("db", "table")).willReturn(Arrays.asList("col1", "col2", "col3"));

        ColumnNameCache cache = new ColumnNameCache(mockColNamesGetter);

        List<List<String>> colNamesList1 = IntStream.range(0, 10) // call 10 times
                .mapToObj(i -> cache.getColumnNames("db", "table"))
                .collect(Collectors.toList());

        cache.invalidate("db", "table");

        List<List<String>> colNamesList2 = IntStream.range(0, 10) // call 10 times
                .mapToObj(i -> cache.getColumnNames("db", "table"))
                .collect(Collectors.toList());

        SoftAssertions.assertSoftly(soft -> {
            colNamesList1.forEach(colNames -> soft.assertThat(colNames).contains("col1", "col2", "col3"));
            colNamesList2.forEach(colNames -> soft.assertThat(colNames).contains("col1", "col2", "col3"));
            then(mockColNamesGetter).should(times(2)).getColumnNames("db", "table");
        });
    }

    @Test
    void invalidateByTableNameOnly_Then_invalidate_All_Database() {
        ColumnNamesGetter mockColNamesGetter = mock(ColumnNamesGetter.class);
        given(mockColNamesGetter.getColumnNames("db1", "table")).willReturn(Arrays.asList("col1", "col2", "col3"));
        given(mockColNamesGetter.getColumnNames("db2", "table")).willReturn(Arrays.asList("col11", "col22", "col33"));

        ColumnNameCache cache = new ColumnNameCache(mockColNamesGetter);

        IntStream.range(0, 10) // call 10 times
                .mapToObj(i -> cache.getColumnNames("db1", "table"))
                .collect(Collectors.toList());

        IntStream.range(0, 10) // call 10 times
                .mapToObj(i -> cache.getColumnNames("db2", "table"))
                .collect(Collectors.toList());

        cache.invalidate(null, "table");

        IntStream.range(0, 10) // call 10 times
                .mapToObj(i -> cache.getColumnNames("db1", "table"))
                .collect(Collectors.toList());

        IntStream.range(0, 10) // call 10 times
                .mapToObj(i -> cache.getColumnNames("db2", "table"))
                .collect(Collectors.toList());

        then(mockColNamesGetter).should(times(2)).getColumnNames("db1", "table");
        then(mockColNamesGetter).should(times(2)).getColumnNames("db2", "table");
    }
}