package mariadbcdc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class QueryDeciderTest {

    @Test
    void decideAlterQuery() {
        assertAlterQuery("alter table user add column aaa char(10)", null, "user");
        assertAlterQuery("alter table test.user add column aaa char(10)", "test", "user");
        assertAlterQuery("alter table member add column aaa char(10)", null, "member");
        assertAlterQuery("alter table mysys.member add column aaa char(10)", "mysys", "member");
        assertAlterQuery("alter  table\nmember\nadd column aaa char(10)", null, "member");
        assertAlterQuery("alter  table\nmysys.member\nadd column aaa char(10)", "mysys", "member");
        assertAlterQuery("\n   alter  table\nmysys.member\nadd column aaa char(10)", "mysys", "member");

        assertNotAlterQuery("# dum");
    }

    private void assertAlterQuery(String alterQuery, String database, String table) {
        AlterQueryDecision alterQueryDecision = QueryDecider.decideAlterQuery(alterQuery);
        assertThat(alterQueryDecision.isAlterQuery()).isTrue();
        assertThat(alterQueryDecision.getDatabase()).isEqualTo(database);
        assertThat(alterQueryDecision.getTable()).isEqualTo(table);
    }

    private void assertNotAlterQuery(String sql) {
        AlterQueryDecision alterQueryDecision = QueryDecider.decideAlterQuery(sql);
        assertThat(alterQueryDecision.isAlterQuery()).isFalse();
        assertThat(alterQueryDecision.getTable()).isNull();
    }
}