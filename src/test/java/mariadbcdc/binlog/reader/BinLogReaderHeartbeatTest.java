package mariadbcdc.binlog.reader;

import mariadbcdc.MariaCdcTestHelper;
import mariadbcdc.Sleeps;
import mariadbcdc.binlog.reader.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.reader.packet.binlog.data.HeartbeatEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class BinLogReaderHeartbeatTest {
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

        reader = new BinLogReader(mariaDB.getHost(), mariaDB.getMappedPort(3306), "cdc", "cdc",
                Duration.ofSeconds(2));
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
    void heartbeat() {
        reader.connect();
        runAsync(() -> reader.start());

        Sleeps.sleep(10);
        assertThat(binLogListener.getHeartbeatTimes().size()).isGreaterThanOrEqualTo(4);
    }

    private class TestBinLogListener implements BinLogListener {
        private List<LocalDateTime> heartbeatTimes = new ArrayList<>();

        @Override
        public void onHeartbeatEvent(BinLogHeader header, HeartbeatEvent data) {
            logger.info("onHeartbeatEvent: {}", LocalDateTime.now());
            heartbeatTimes.add(LocalDateTime.now());
        }

        public List<LocalDateTime> getHeartbeatTimes() {
            return heartbeatTimes;
        }
    }

    private void runAsync(Runnable runnable) {
        new Thread(runnable).start();
    }
}
