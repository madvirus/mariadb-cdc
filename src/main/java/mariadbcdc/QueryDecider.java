package mariadbcdc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryDecider {
    private static Pattern pattern = Pattern.compile("alter\\s+table\\s+((\\S+)\\.)?(\\S+)\\s+");

    public static AlterQueryDecision decideAlterQuery(String sql) {
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            return new AlterQueryDecision(true, matcher.group(2), matcher.group(3));
        } else {
            return new AlterQueryDecision(false, null, null);
        }
    }
}
