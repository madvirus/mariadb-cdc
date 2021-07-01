package mariadbcdc.binlog;

import mariadbcdc.BinlogPosition;
import mariadbcdc.MariaCdcTestHelper;
import mariadbcdc.Sleeps;
import mariadbcdc.binlog.packet.binlog.BinLogHeader;
import mariadbcdc.binlog.packet.binlog.data.RotateEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class BinLogReaderPositionTest {
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

        reader = new BinLogReader(mariaDB.getHost(), mariaDB.getMappedPort(3306), "cdc", "cdc");
    }

    @AfterEach
    void tearDown() {
        if (reader != null) {
            reader.disconnect();
        }
    }

    @Test
    void getPosition() {
        helper.insertMember("이름1", "이메일1");
        helper.insertMember("이름2", "이메일2");
        helper.insertMember("이름3", "이메일3");
        BinlogPosition realPos = helper.getCurrentPosition();

        reader.connect();
        BinlogPosition binlogPosition = reader.getPosition();
        logger.info("reader got binlog position: {}", binlogPosition);
        assertThat(binlogPosition).isEqualTo(realPos);
    }

    @Test
    void startWithPosition() {
        helper.insertMember("이름4", "이메일4");
        BinlogPosition realPos = helper.getCurrentPosition();

        reader.setStartBinlogPosition(realPos.getFilename(), 4L);
        reader.connect();

        AtomicReference<RotateEvent> evtRef = new AtomicReference<>();
        reader.setBinLogListener(new BinLogListener() {
            @Override
            public void onRotateEvent(BinLogHeader header, RotateEvent data) {
                evtRef.set(data);
            }
        });
        runAsync(() -> {
            try {
                reader.start();
            } catch (Exception ex) {
            }
        });
        Sleeps.sleep(2);
        RotateEvent rotateEvent = evtRef.get();
        assertThat(rotateEvent.getFilename()).isEqualTo(realPos.getFilename());
        assertThat(rotateEvent.getPosition()).isEqualTo(4L);
    }

    @Test
    void binLogPositionTraced() {
        helper.truncateMember();
        reader.connect();
        runAsync(() -> {
            try {
                reader.start();
            } catch (Exception ex) {
            }
        });
        Sleeps.sleep(1);
        helper.insertMember(1, "이름1");
        helper.insertMember(2, "이름2");
        helper.insertMember(3, "이름3");
        helper.insertMember(4, "이름4");

        Sleeps.sleep(2);
        BinlogPosition position = reader.getPosition();
        BinlogPosition realPos = helper.getCurrentPosition();
        assertThat(position).isEqualTo(realPos);
    }

    private void runAsync(Runnable runnable) {
        new Thread(runnable).start();
    }
}
