package mariadbcdc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryDecider {
    private static Pattern alterPattern = Pattern.compile("alter\\s+table\\s+((`?\\S+`?)\\.)?`?(\\S+)`?\\s+", Pattern.CASE_INSENSITIVE);

    private static Pattern renamePattern =
            Pattern.compile("rename\\s+table\\s+", Pattern.CASE_INSENSITIVE);

    private static Pattern dropPattern =
            Pattern.compile("drop\\s+table\\s+" +
                    "(if\\s+exists)?" +
                    "(.*)" +
                    "(wait [0-9]+|nowait)?" +
                    "\\s*(restrict|cascade)?", Pattern.CASE_INSENSITIVE);

    public static SchemaChangeQueryDecision decideSchemaChangeQuery(String sql) {
        Matcher alterMatcher = alterPattern.matcher(sql);
        if (alterMatcher.find()) {
            return new SchemaChangeQueryDecision(true,
                    Collections.singletonList(new SchemaChangedTable(
                            removeBacktick(alterMatcher.group(2)),
                            removeBacktick(alterMatcher.group(3))))
            );
        }
        Matcher renameMatcher = renamePattern.matcher(sql);
        if (renameMatcher.find()) {
            return handleRename(sql, renameMatcher);
        }
        Matcher dropMatcher = dropPattern.matcher(sql);
        if (dropMatcher.find()) {
            return handleDrop(sql, dropMatcher);
        }
        return new SchemaChangeQueryDecision(false, Collections.emptyList());
    }

    private static String removeBacktick(String identifier) {
        return identifier == null ? null :
                identifier.replace("`", "");
    }

    private static Pattern renameSubPattern =
            Pattern.compile(
                    "\\s*(`?(\\S+)`?\\.)?`?([^\\s`]+)`?\\s+" +
                            "((wait [0-9]+|nowait)\\s+)?" +
                            "to\\s+(`?(\\S+)`?\\.)?`?([^\\s`]+)`?", Pattern.CASE_INSENSITIVE);

    private static SchemaChangeQueryDecision handleRename(String sql, Matcher renameMatcher) {
        String queryPart = sql.substring(renameMatcher.end());
        List<SchemaChangedTable> tables = new ArrayList<>();
        Matcher n2nMatcher = renameSubPattern.matcher(queryPart);
        if (n2nMatcher.find()) {
            do {
                String database = removeBacktick(n2nMatcher.group(2));
                String table = removeBacktick(n2nMatcher.group(3));
                tables.add(new SchemaChangedTable(database, table));
            } while (n2nMatcher.find());
        }
        return new SchemaChangeQueryDecision(true, tables);
    }

    private static Pattern dropNamePattern =
            Pattern.compile("\\s*(`?([^`\\s]+)`?\\.)?`?([^`\\s,]+)`?", Pattern.CASE_INSENSITIVE);

    private static SchemaChangeQueryDecision handleDrop(String sql, Matcher dropMatcher) {
        List<SchemaChangedTable> tables = new ArrayList<>();
        String queryPart = dropMatcher.group(2);
        if (queryPart != null) {
            Matcher nameMatcher = dropNamePattern.matcher(queryPart);
            if (nameMatcher.find()) {
                do {
                    String database = nameMatcher.group(2);
                    String table = nameMatcher.group(3);
                    tables.add(new SchemaChangedTable(database, table));
                } while (nameMatcher.find());
            }
        }
        return new SchemaChangeQueryDecision(true, tables);
    }
}
