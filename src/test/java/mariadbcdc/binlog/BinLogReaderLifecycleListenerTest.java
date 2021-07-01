package mariadbcdc.binlog;

import mariadbcdc.MariaCdcTestHelper;
import mariadbcdc.Sleeps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class BinLogReaderLifecycleListenerTest {
    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer("mariadb:10.3")
            .withConfigurationOverride("conf.d.103")
            .withInitScript("init.sql");

    private Logger logger = LoggerFactory.getLogger(getClass());

    MariaCdcTestHelper helper;
    private BinLogReader reader;

    @BeforeEach
    void setUp() {
        helper = new MariaCdcTestHelper(mariaDB);
        helper.createCdcUser("cdc", "cdc");
    }

    @Test
    void lifecycle() {
        reader = new BinLogReader(mariaDB.getHost(), mariaDB.getMappedPort(3306), "cdc", "cdc");
        TestLifecycleListener lcl = new TestLifecycleListener();
        try {
            reader.setBinLogLifecycleListener(lcl);
            reader.connect();

            assertThat(lcl.connectedCalled()).isTrue();

            runAsync(() -> reader.start());

            Sleeps.sleep(1);
            assertThat(lcl.startedCalled()).isTrue();
        } finally {
            reader.disconnect();
            assertThat(lcl.disconnectedCalled()).isTrue();
        }
    }

    private void runAsync(Runnable runnable) {
        new Thread(runnable).start();
    }

    public static class TestLifecycleListener implements BinLogLifecycleListener {
        private boolean connected = false;
        private boolean started = false;
        private boolean disconnected = false;

        public boolean connectedCalled() {
            return connected;
        }

        public boolean startedCalled() {
            return started;
        }

        public boolean disconnectedCalled() {
            return disconnected;
        }

        @Override
        public void onConnected() {
            connected = true;
        }

        @Override
        public void onStarted() {
            started = true;
        }

        @Override
        public void onDisconnected() {
            disconnected = true;
        }
    }
}
