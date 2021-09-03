package mariadbcdc.binlog;

import mariadbcdc.*;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class MariadbCdc_Binlog_BadSavedPosition_Test {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer("mariadb:10.3")
            .withConfigurationOverride("conf.d.103")
            .withInitScript("init.sql");

    private ColumnNamesGetter columnNamesGetter;
    private MariaCdcTestHelper helper;

    @BeforeEach
    void setUp() {
        helper = new MariaCdcTestHelper(mariaDB);
        helper.createCdcUser("cdc", "cdc");
        columnNamesGetter = new JdbcColumnNamesGetter("localhost",
                helper.port(), helper.cdcUser(), helper.cdcPassword());
    }

    @Test
    void withBadPositionFile_Then_Stopped() throws IOException {
        helper.insertMember("name1", "email1");
        helper.insertMember("name2", "email2");

        BinlogPosition binPos = helper.getCurrentPosition();
        Path path = Paths.get("temp/pos.file");
        Files.write(path, Arrays.asList(binPos.getFilename() + "/" + binPos.getPosition() + 50000L), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        MariadbCdcConfig config = createConfig("temp/pos.file");
        MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);

        List<String> events = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void started(BinlogPosition bp) {
                logger.info("started: {}", bp.getStringFormat());
                events.add("started");
            }

            @Override
            public void startFailed(Exception e) {
                logger.info("start failed: {}", e.getMessage());
                events.add("start failed: " + e.getMessage());
            }

            @Override
            public void stopped() {
                logger.info("stopped");
                events.add("stopped");
            }
        });

        cdc.start();
        Sleeps.sleep(1);
        cdc.stop();

        assertThat(events.get(1)).startsWith("start failed:");
    }

    @Test
    void usingCurrentPosition_withBadPositionFile_Then_Start() throws IOException {
        helper.insertMember("name1", "email1");
        helper.insertMember("name2", "email2");

        BinlogPosition binPos = helper.getCurrentPosition();
        Path path = Paths.get("temp/pos.file");
        Files.write(path, Arrays.asList(binPos.getFilename() + "/" + binPos.getPosition() + 50000L), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        MariadbCdcConfig config = createConfig("temp/pos.file");
        config.setUsingLastPositionWhenBadPosition(true);
        MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);

        List<RowChangedData> result = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void onDataChanged(List<RowChangedData> list) {
                result.addAll(list);
            }
        });

        cdc.start();
        Sleeps.sleep(1);
        helper.insertMember("name3", "email3");
        Sleeps.sleep(1);
        helper.insertMember("name4", "email4");
        cdc.stop();

        SoftAssertions.assertSoftly(s -> {
            s.assertThat(result).hasSize(2);
            s.assertThat(result.get(0).getDataRow().getString("name")).isEqualTo("name3");
            s.assertThat(result.get(0).getDataRow().getString("email")).isEqualTo("email3");
            s.assertThat(result.get(1).getDataRow().getString("name")).isEqualTo("name4");
            s.assertThat(result.get(1).getDataRow().getString("email")).isEqualTo("email4");
        });
    }

    private MariadbCdcConfig createConfig(String posFilePath) {
        return helper.createMariadbCdcConfigUsingConnectorFactory(posFilePath);
    }

}
