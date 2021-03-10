package mariadbcdc;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class MariadbCdc_103_Basic_Test {
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
    void testSet() throws IOException {
        startedAndStopped();
        fromSavedPosition();
        saveBinlogPositionAfterRead();
        handleInsertedData();
        handleUpdatedData();
        handleDeletedData();
    }

    void startedAndStopped() {
        helper.deleteSavedPositionFile("temp/pos.file");
        MariadbCdcConfig config = helper.createMariadbCdcConfig("temp/pos.file");
        MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);

        List<String> events = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void started(BinlogPosition bp) {
                events.add("started");
            }

            @Override
            public void stopped() {
                events.add("stopped");
            }
        });

        cdc.start();
        Sleeps.sleep(1);
        cdc.stop();
        Sleeps.sleep(1);

        assertThat(events).containsExactly("started", "stopped");
    }

    void fromSavedPosition() {
        helper.insertMember("name1", "email1");
        helper.insertMember("name2", "email2");

        BinlogPosition binPos = helper.getCurrentPosition();
        helper.saveCurrentPosition("temp/pos.saved");

        MariadbCdcConfig config = helper.createMariadbCdcConfig("temp/pos.saved");
        MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);

        List<BinlogPosition> pos = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void started(BinlogPosition position) {
                pos.add(position);
            }
        });
        cdc.start();
        helper.insertMember("name3", "email3");
        Sleeps.sleep(1);
        helper.insertMember("name4", "email4");
        Sleeps.sleep(1);
        cdc.stop();

        BinlogPosition p = pos.get(0);
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(p.getFilename()).isEqualTo(binPos.getFilename());
            soft.assertThat(p.getPosition()).isEqualTo(binPos.getPosition());
        });
    }

    void saveBinlogPositionAfterRead() throws IOException {
        helper.deleteSavedPositionFile("temp/pos.last");
        MariadbCdcConfig config = helper.createMariadbCdcConfig("temp/pos.last");
        MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);
        cdc.start();
        helper.insertMember("name5", "email5");
        Sleeps.sleep(1);
        helper.insertMember("name6", "email6");
        Sleeps.sleep(1);
        helper.insertMember("name7", "email7");
        Sleeps.sleep(1);
        cdc.stop();

        BinlogPosition binPos = helper.getCurrentPosition();
        List<String> lines = Files.readAllLines(Paths.get("temp/pos.last"));
        assertThat(lines.get(0)).isEqualTo(binPos.getStringFormat());
    }

    void handleInsertedData() {
        helper.deleteSavedPositionFile("temp/pos.saved");

        MariadbCdcConfig config = helper.createMariadbCdcConfig("temp/pos.saved");
        config.setLocalDateTimeAdjustingHour(-9);
        MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);

        List<RowChangedData> result = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void onDataChanged(List<RowChangedData> list) {
                result.addAll(list);
            }
        });
        cdc.start();
        helper.insertMember("name3", "email3", LocalDateTime.of(2021, 3, 10, 1, 2, 3));
        Sleeps.sleep(1);
        helper.insertMember("name4", "email4");
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result.get(0).getType()).isEqualTo(ChangeType.INSERT);
            soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(0).getTable()).isEqualTo("member");
            soft.assertThat(result.get(0).getDataRow().getLong("id")).isNotNull();
            soft.assertThat(result.get(0).getDataRow().getString("name")).isEqualTo("name3");
            soft.assertThat(result.get(0).getDataRow().getString("email")).isEqualTo("email3");
            soft.assertThat(result.get(0).getDataRow().getLocalDateTime("reg"))
                    .isEqualTo(LocalDateTime.of(2021, 3, 10, 1, 2, 3));
            soft.assertThat(result.get(0).getBinLogPosition()).isNotNull();

            soft.assertThat(result.get(1).getType()).isEqualTo(ChangeType.INSERT);
            soft.assertThat(result.get(1).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(1).getTable()).isEqualTo("member");
            soft.assertThat(result.get(1).getDataRow().getLong("id")).isNotNull();
            soft.assertThat(result.get(1).getDataRow().getString("name")).isEqualTo("name4");
            soft.assertThat(result.get(1).getDataRow().getString("email")).isEqualTo("email4");
            soft.assertThat(result.get(1).getBinLogPosition()).isNotNull();
        });
    }

    private void handleUpdatedData() throws IOException {
        helper.deleteSavedPositionFile("temp/pos.saved");
        MariadbCdcConfig config = helper.createMariadbCdcConfig("temp/pos.saved");
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
        helper.updateMembers(TestPair.of(3L, "nameupd3"), TestPair.of(4L, "nameupd4"));
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result.get(0).getType()).isEqualTo(ChangeType.UPDATE);
            soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(0).getTable()).isEqualTo("member");
            soft.assertThat(result.get(0).getDataRow().getLong("id")).isEqualTo(3L);
            soft.assertThat(result.get(0).getDataRow().getString("name")).isEqualTo("nameupd3");
            soft.assertThat(result.get(0).getBinLogPosition()).isNotNull();
            soft.assertThat(result.get(1).getType()).isEqualTo(ChangeType.UPDATE);
            soft.assertThat(result.get(1).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(1).getTable()).isEqualTo("member");
            soft.assertThat(result.get(1).getDataRow().getLong("id")).isEqualTo(4L);
            soft.assertThat(result.get(1).getDataRow().getString("name")).isEqualTo("nameupd4");
            soft.assertThat(result.get(1).getBinLogPosition()).isNotNull();
        });
    }

    private void handleDeletedData() {
        helper.insertMember("name5", "email5");
        helper.insertMember("name6", "email6");

        helper.deleteSavedPositionFile("temp/pos.saved");
        MariadbCdcConfig config = helper.createMariadbCdcConfig("temp/pos.saved");
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
        helper.deleteMembers(3L, 4L, 5L);
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result).hasSize(3);
            if (result.size() == 3) {
                soft.assertThat(result.get(0).getType()).isEqualTo(ChangeType.DELETE);
                soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
                soft.assertThat(result.get(0).getTable()).isEqualTo("member");
                soft.assertThat(result.get(0).getDataRow().getLong("id")).isEqualTo(3L);
                soft.assertThat(result.get(0).getBinLogPosition()).isNotNull();

                soft.assertThat(result.get(1).getType()).isEqualTo(ChangeType.DELETE);
                soft.assertThat(result.get(1).getDatabase()).isEqualTo("test");
                soft.assertThat(result.get(1).getTable()).isEqualTo("member");
                soft.assertThat(result.get(1).getDataRow().getLong("id")).isEqualTo(4L);
                soft.assertThat(result.get(1).getBinLogPosition()).isNotNull();

                soft.assertThat(result.get(2).getType()).isEqualTo(ChangeType.DELETE);
                soft.assertThat(result.get(2).getDatabase()).isEqualTo("test");
                soft.assertThat(result.get(2).getTable()).isEqualTo("member");
                soft.assertThat(result.get(2).getDataRow().getLong("id")).isEqualTo(5L);
                soft.assertThat(result.get(2).getBinLogPosition()).isNotNull();
            }
        });
    }

}
