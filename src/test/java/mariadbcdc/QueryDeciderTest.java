package mariadbcdc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class QueryDeciderTest {

    @Test
    void alterTableQuery() {
        assertSchemaChanged("alter table user add column aaa char(10)",
                new SchemaChangedTable(null, "user"));
        assertSchemaChanged("alter table test.user add column aaa char(10)",
                new SchemaChangedTable("test", "user"));
        assertSchemaChanged("alter table member add column aaa char(10)",
                new SchemaChangedTable(null, "member"));
        assertSchemaChanged("alter table mysys.member add column aaa char(10)",
                new SchemaChangedTable("mysys", "member"));
        assertSchemaChanged("alter  table\nmember\nadd column aaa char(10)",
                new SchemaChangedTable(null, "member"));
        assertSchemaChanged("alter  table\nmysys.member\nadd column aaa char(10)",
                new SchemaChangedTable("mysys", "member"));
        assertSchemaChanged("\n   alter  table\nmysys.member\nadd column aaa char(10)",
                new SchemaChangedTable("mysys", "member"));
        assertSchemaChanged("ALTER TABLE `mysys`.`member` \n" +
                "CHANGE COLUMN `recmsg` `recmsg` VARCHAR(100) NULL DEFAULT NULL COMMENT '결제결과메세지' AFTER `invoice_no`",
                new SchemaChangedTable("mysys", "member"));
        assertSchemaChanged("ALTER TABLE mysys.`member` \n" +
                "CHANGE COLUMN `recmsg` `recmsg` VARCHAR(100) NULL DEFAULT NULL COMMENT '결제결과메세지' AFTER `invoice_no`",
                new SchemaChangedTable("mysys", "member"));
        assertSchemaChanged("ALTER TABLE `mysys`.member \n" +
                "CHANGE COLUMN `recmsg` `recmsg` VARCHAR(100) NULL DEFAULT NULL COMMENT '결제결과메세지' AFTER `invoice_no`",
                new SchemaChangedTable("mysys", "member"));
        assertNotAlterQuery("# dum");
    }

    @Test
    void renameTableQuery() {
        assertSchemaChanged("rename table old to new",
                new SchemaChangedTable(null, "old"));
        assertSchemaChanged("rename table `old` to `new`",
                new SchemaChangedTable(null, "old"));
        assertSchemaChanged("RENAME TABLE old TO new",
                new SchemaChangedTable(null, "old"));
        assertSchemaChanged("rename table IF exists old to new",
                new SchemaChangedTable(null, "old"));
        assertSchemaChanged("rename table test.old to test.new",
                new SchemaChangedTable("test", "old"));
        assertSchemaChanged("rename table old1 to new1, old2 to new2",
                new SchemaChangedTable(null, "old1"),
                new SchemaChangedTable(null, "old2")
                );
        assertSchemaChanged("rename table test.old1 to test.new1, test2.old2 to new2",
                new SchemaChangedTable("test", "old1"),
                new SchemaChangedTable("test2", "old2")
                );

        assertSchemaChanged("rename table `test`.`old` to `test`.`new`",
                new SchemaChangedTable("test", "old"));
    }

    @Test
    void dropTableQuery() {
        assertSchemaChanged("drop table test.member",
                new SchemaChangedTable("test", "member"));
        assertSchemaChanged("drop table test.member, user",
                new SchemaChangedTable("test", "member"),
                new SchemaChangedTable(null, "user")
        );

        assertSchemaChanged("drop table `test`.`member`",
                new SchemaChangedTable("test", "member"));
    }

    private void assertSchemaChanged(String alterQuery, SchemaChangedTable ... expected) {
        SchemaChangeQueryDecision schemaChangeQueryDecision = QueryDecider.decideSchemaChangeQuery(alterQuery);
        assertThat(schemaChangeQueryDecision.isAlterQuery()).isTrue();
        assertThat(schemaChangeQueryDecision.getDatabaseTableNames()).hasSize(expected.length);
        for (int i = 0 ; i < expected.length ; i++) {
            assertThat(schemaChangeQueryDecision.getDatabaseTableNames().get(i)).isEqualTo(expected[i]);
        }
    }

    private void assertNotAlterQuery(String sql) {
        SchemaChangeQueryDecision schemaChangeQueryDecision = QueryDecider.decideSchemaChangeQuery(sql);
        assertThat(schemaChangeQueryDecision.isAlterQuery()).isFalse();
        assertThat(schemaChangeQueryDecision.getDatabaseTableNames()).isEmpty();
    }
}