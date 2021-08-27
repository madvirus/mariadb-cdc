package mariadbcdc.binlog.reader;

import mariadbcdc.MariaCdcTestHelper;
import mariadbcdc.Sleeps;
import mariadbcdc.binlog.reader.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.reader.packet.binlog.data.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class BinLogReaderReconnectTest {
    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer("mariadb:10.3")
            .withConfigurationOverride("conf.d.103")
            .withInitScript("init.sql");

    private Logger logger = LoggerFactory.getLogger(getClass());

    MariaCdcTestHelper helper;

    @BeforeEach
    void setUp() {
        helper = new MariaCdcTestHelper(mariaDB);
        helper.createCdcUser("cdc", "cdc");
    }

    @Test
    void reconnect() {
        helper.truncateMember();
        BinLogReader reader1 = new BinLogReader(mariaDB.getHost(), mariaDB.getMappedPort(3306), "cdc", "cdc");
        reader1.setReaderId("READER1");
        try {
            TestBinLogListener binLogListener = new TestBinLogListener();
            reader1.setBinLogListener(binLogListener);

            reader1.setSlaveServerId(9999L);
            reader1.setHeartbeatPeriod(Duration.ofSeconds(2));
            reader1.enableReconnection();
            reader1.setKeepConnectionTimeout(Duration.ofSeconds(5));

            reader1.connect();
            runAsync(() -> reader1.start());
            Sleeps.sleep(1);
            helper.insertMember(1L, "name1");  // 연걸 끊기기 전 insert

            // 같은 슬레이브ID를 사용하는 다른 리더를 연결해서 강제 접속 해제
            Sleeps.sleep(2);
            BinLogReader reader2 = new BinLogReader(mariaDB.getHost(), mariaDB.getMappedPort(3306), "cdc", "cdc");
            reader2.setReaderId("READER2");
            reader2.setSlaveServerId(9999L);
            reader2.connect();
            runAsync(() -> reader2.start());
            Sleeps.sleep(2);
            reader2.disconnect(); // 다른 리더 종료
            Sleeps.sleep(1);

            helper.insertMember(2L, "name2");  // 연결 끊기고 추가

            Sleeps.sleep(3);

            helper.insertMember(3L, "name3");  // 재연결후 추가

            Sleeps.sleep(3);

            assertThat(reader1.isReading()).isTrue();
            assertThat(binLogListener.getRows()).hasSize(3);
            assertThat(binLogListener.getRows().get(0)[0]).isEqualTo(1L);
            assertThat(binLogListener.getRows().get(0)[1]).isEqualTo("name1");
            assertThat(binLogListener.getRows().get(1)[1]).isEqualTo("name2");
            assertThat(binLogListener.getRows().get(2)[1]).isEqualTo("name3");
        } finally {
            if (reader1 != null) reader1.disconnect();
        }
    }

    @Test
    void noreconnect() {
        helper.truncateMember();
        BinLogReader reader1 = new BinLogReader(mariaDB.getHost(), mariaDB.getMappedPort(3306), "cdc", "cdc");
        reader1.setReaderId("READER1");
        try {
            TestBinLogListener binLogListener = new TestBinLogListener();
            reader1.setBinLogListener(binLogListener);

            reader1.setSlaveServerId(9999L);
            reader1.setHeartbeatPeriod(Duration.ofSeconds(2));

            reader1.connect();
            runAsync(() -> reader1.start());
            Sleeps.sleep(1);
            helper.insertMember(1L, "name1");  // 연걸 끊기기 전 insert

            // 같은 슬레이브ID를 사용하는 다른 리더를 연결해서 강제 접속 해제
            Sleeps.sleep(2);
            BinLogReader reader2 = new BinLogReader(mariaDB.getHost(), mariaDB.getMappedPort(3306), "cdc", "cdc");
            reader2.setReaderId("READER2");
            reader2.setSlaveServerId(9999L);
            reader2.connect();
            runAsync(() -> reader2.start());
            Sleeps.sleep(2);
            reader2.disconnect(); // 다른 리더 종료
            Sleeps.sleep(1);

            helper.insertMember(2L, "name2");  // 연결 끊기고 추가

            Sleeps.sleep(2);

            helper.insertMember(3L, "name3");  // 연결 끊기고 추가

            Sleeps.sleep(2);

            assertThat(reader1.isReading()).isFalse();
            assertThat(binLogListener.getRows()).hasSize(1);
            assertThat(binLogListener.getRows().get(0)[0]).isEqualTo(1L);
            assertThat(binLogListener.getRows().get(0)[1]).isEqualTo("name1");
        } finally {
            if (reader1 != null) reader1.disconnect();
        }
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
