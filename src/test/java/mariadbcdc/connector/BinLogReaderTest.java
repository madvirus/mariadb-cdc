package mariadbcdc.connector;

import mariadbcdc.MariaCdcTestHelper;
import mariadbcdc.Sleeps;
import mariadbcdc.TestPair;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.data.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class BinLogReaderTest {
    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer("mariadb:10.3")
            .withConfigurationOverride("conf.d.103")
            .withInitScript("init.sql");

    private Logger logger = LoggerFactory.getLogger(getClass());

    MariaCdcTestHelper helper;
    private BinLogReader reader;
    private TestBinLogListener binLogListener;

    @BeforeEach
    void setUp() {
        helper = new MariaCdcTestHelper(mariaDB);
        helper.createCdcUser("cdc", "cdc");

        reader = new BinLogReader(mariaDB.getHost(), mariaDB.getMappedPort(3306), "cdc", "cdc");
        binLogListener = new TestBinLogListener();
        reader.setBinLogListener(binLogListener);
    }

    @AfterEach
    void tearDown() {
        if (reader != null) {
            reader.disconnect();
        }
    }

    @Test
    void insert() {
        helper.truncateMember();
        reader.connect();
        runAsync(() -> reader.start());

        Sleeps.sleep(1);
        helper.withId(1L).withName("마리아DB")
                .withAgree(true)
                .withBirthday(LocalDate.of(2020, 12, 31))
                .withReg(LocalDateTime.of(2021, 1, 14, 20, 30, 15))
                .insert();
        Sleeps.sleep(1);

        assertThat(binLogListener.rows.get(0)[0]).isEqualTo(1L);
        assertThat(binLogListener.rows.get(0)[1]).isEqualTo("마리아DB");
        assertThat(binLogListener.rows.get(0)[4]).isEqualTo(1);
        assertThat(binLogListener.rows.get(0)[6]).isEqualTo(LocalDate.of(2020, 12, 31));
        assertThat(binLogListener.rows.get(0)[7]).isEqualTo(LocalDateTime.of(2021, 1, 14, 20, 30, 15));
    }

    @Test
    void update() {
        helper.truncateMember();
        helper.insertMember(1L, "name");

        reader.connect();

        runAsync(() -> reader.start());
        Sleeps.sleep(1);

        String post = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        helper.updateMembers(TestPair.of(1L, "이름" + post));
        logger.info("update Members");

        Sleeps.sleep(1);

        assertThat(binLogListener.rows.get(0)[0]).isEqualTo(1L);
        assertThat(binLogListener.rows.get(0)[1]).isEqualTo("이름" + post);
        assertThat(binLogListener.beforeRows.get(0)[0]).isEqualTo(1L);
        assertThat(binLogListener.beforeRows.get(0)[1]).isEqualTo("name");
    }

    @Test
    void delete() {
        helper.truncateMember();
        helper.insertMember(1L, "삭제대상");

        reader.connect();

        runAsync(() -> reader.start());
        Sleeps.sleep(1);

        helper.deleteMembers(1L);

        Sleeps.sleep(1);

        assertThat(binLogListener.rows.get(0)[0]).isEqualTo(1L);
        assertThat(binLogListener.rows.get(0)[1]).isEqualTo("삭제대상");
    }

    private class TestBinLogListener implements BinLogListener {
        private List<Object[]> rows = new ArrayList<>();
        private List<Object[]> beforeRows = new ArrayList<>();

        public List<Object[]> getRows() {
            return rows;
        }

        public List<Object[]> getBeforeRows() {
            return beforeRows;
        }

        @Override
        public void onRotateEvent(BinLogHeader header, RotateEvent data) {
            logger.info("rotate event: {}", data);
        }

        @Override
        public void onTableMapEvent(BinLogHeader header, TableMapEvent data) {
            logger.info("tableMap: {}", data);
        }

        @Override
        public void onWriteRowsEvent(BinLogHeader header, WriteRowsEvent data) {
            data.getRows().forEach(row -> rows.add(row));
        }

        @Override
        public void onUpdateRowsEvent(BinLogHeader header, UpdateRowsEvent data) {
            logger.info("updateRows: tableId={}, rows={}", data.getTableId(), data.getPairs().size());
            data.getPairs().forEach(rp -> {
                logger.info("before={}, after={}", rp.getBefore(), rp.getAfter());
                rows.add(rp.getAfter());
                beforeRows.add(rp.getBefore());
            });
        }

        @Override
        public void onDeleteRowsEvent(BinLogHeader header, DeleteRowsEvent data) {
            data.getRows().forEach(row -> rows.add(row));
        }

        @Override
        public void onXidEvent(BinLogHeader header, XidEvent data) {
            logger.info("xid: {}", data);
        }
    }

    private void runAsync(Runnable runnable) {
        new Thread(runnable).start();
    }
}
