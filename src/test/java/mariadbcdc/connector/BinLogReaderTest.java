package mariadbcdc.connector;

import mariadbcdc.MariaCdcTestHelper;
import mariadbcdc.Sleeps;
import mariadbcdc.TestPair;
import mariadbcdc.connector.packet.binlog.BinLogHeader;
import mariadbcdc.connector.packet.binlog.data.RotateEvent;
import mariadbcdc.connector.packet.binlog.data.TableMapEvent;
import mariadbcdc.connector.packet.binlog.data.UpdateRowsEvent;
import mariadbcdc.connector.packet.binlog.data.XidEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BinLogReaderTest {
    private Logger logger = LoggerFactory.getLogger(getClass());
    MariaCdcTestHelper helper = new MariaCdcTestHelper("localhost", 3306, "root", "root", "root");
    private BinLogReader reader;

    @BeforeEach
    void setUp() {
        reader = new BinLogReader("localhost", 3306, "root", "root");
    }

    @AfterEach
    void tearDown() {
        if (reader != null) {
            reader.disconnect();
        }
    }

    @Test
    void todo() {
        helper.truncateMember();
        helper.insertMember(1L, "name");

        BinLogReader reader = new BinLogReader("localhost", 3306, "root", "root");
        BinLogListener binLogListener = new TestBinLogListener();
        reader.setBinLogListener(binLogListener);
        reader.connect();

        runAsync(() -> reader.start());
        Sleeps.sleep(2);
        try {
            String post = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            helper.updateMembers(TestPair.of(1L, "이름" + post));
            logger.info("update Members");
        } catch (Exception e) {
            logger.error("fail to update Members: ", e);
        }

        Sleeps.sleep(10);
    }

    private class TestBinLogListener implements BinLogListener {
        @Override
        public void onRotateEvent(BinLogHeader header, RotateEvent data) {
            logger.info("rotate event: {}", data);
        }

        @Override
        public void onTableMapEvent(BinLogHeader header, TableMapEvent data) {
            logger.info("tableMap: {}", data);
        }

        @Override
        public void onUpdateRowsEvent(BinLogHeader header, UpdateRowsEvent data) {
            logger.info("updateRows: tableId={}, rows={}", data.getTableId(), data.getPairs().size());
            data.getPairs().forEach(rp -> {
                logger.info("before={}, after={}", rp.getBefore(), rp.getAfter());
            });
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
