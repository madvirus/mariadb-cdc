package mariadbcdc.binlog;

import mariadbcdc.*;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

@Tag("integration")
@Testcontainers
public class MariadbCdc_Binlog_105_FullRowMetadata_Test {
    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer("mariadb:10.5")
            .withConfigurationOverride("conf.d.105")
            .withInitScript("init.sql");

    private MariaCdcTestHelper helper;

    @BeforeEach
    void setUp() {
        helper = new MariaCdcTestHelper(mariaDB);
        helper.createCdcUser("cdc", "cdc");
    }

    @Test
    void testSet() {
        handleInsertedData();
        handleUpdatedData();
        handleDeletedData();
    }

    void handleInsertedData() {
        helper.deleteSavedPositionFile("temp/pos.saved");
        MariadbCdcConfig config = createConfig("temp/pos.saved");
        MariadbCdc cdc = new MariadbCdc(config);

        List<RowChangedData> result = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void onDataChanged(List<RowChangedData> list) {
                result.addAll(list);
            }
        });
        cdc.start();
        helper.insertMember("name1", "email1");
        Sleeps.sleep(1);
        helper.insertMember("name2", "email2");
        helper.insertMember("name3", "email3");
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result.get(0).getType()).isEqualTo(ChangeType.INSERT);
            soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(0).getTable()).isEqualTo("member");
            soft.assertThat(result.get(0).getDataRow().getString("name")).isEqualTo("name1");
            soft.assertThat(result.get(0).getDataRow().getString("email")).isEqualTo("email1");
            soft.assertThat(result.get(1).getType()).isEqualTo(ChangeType.INSERT);
            soft.assertThat(result.get(1).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(1).getTable()).isEqualTo("member");
            soft.assertThat(result.get(1).getDataRow().getString("name")).isEqualTo("name2");
            soft.assertThat(result.get(1).getDataRow().getString("email")).isEqualTo("email2");
            soft.assertThat(result.get(2).getType()).isEqualTo(ChangeType.INSERT);
            soft.assertThat(result.get(2).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(2).getTable()).isEqualTo("member");
            soft.assertThat(result.get(2).getDataRow().getString("name")).isEqualTo("name3");
            soft.assertThat(result.get(2).getDataRow().getString("email")).isEqualTo("email3");
        });
    }

    private void handleUpdatedData() {
        helper.deleteSavedPositionFile("temp/pos.saved");
        MariadbCdcConfig config = createConfig("temp/pos.saved");
        MariadbCdc cdc = new MariadbCdc(config);
        List<RowChangedData> result = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void onDataChanged(List<RowChangedData> list) {
                result.addAll(list);
            }
        });

        cdc.start();
        Sleeps.sleep(1);
        helper.updateMembers(TestPair.of(2L, "nameupd2"), TestPair.of(3L, "nameupd3"));
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result.get(0).getType()).isEqualTo(ChangeType.UPDATE);
            soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(0).getTable()).isEqualTo("member");
            soft.assertThat(result.get(0).getDataRow().getLong("id")).isEqualTo(2L);
            soft.assertThat(result.get(0).getDataRow().getString("name")).isEqualTo("nameupd2");
            soft.assertThat(result.get(0).getDataRow().getString("email")).isEqualTo("email2");
            soft.assertThat(result.get(1).getType()).isEqualTo(ChangeType.UPDATE);
            soft.assertThat(result.get(1).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(1).getTable()).isEqualTo("member");
            soft.assertThat(result.get(1).getDataRow().getLong("id")).isEqualTo(3L);
            soft.assertThat(result.get(1).getDataRow().getString("name")).isEqualTo("nameupd3");
            soft.assertThat(result.get(1).getDataRow().getString("email")).isEqualTo("email3");
        });
    }

    private void handleDeletedData() {
        helper.insertMember("name4", "email4");
        helper.insertMember("name5", "email5");

        helper.deleteSavedPositionFile("temp/pos.saved");
        MariadbCdcConfig config = createConfig("temp/pos.saved");
        MariadbCdc cdc = new MariadbCdc(config);
        List<RowChangedData> result = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void onDataChanged(List<RowChangedData> list) {
                result.addAll(list);
            }
        });

        cdc.start();
        Sleeps.sleep(1);
        helper.deleteMembers(3L, 4L);
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result.get(0).getType()).isEqualTo(ChangeType.DELETE);
            soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(0).getTable()).isEqualTo("member");
            soft.assertThat(result.get(0).getDataRow().getLong("id")).isEqualTo(3L);
            soft.assertThat(result.get(1).getType()).isEqualTo(ChangeType.DELETE);
            soft.assertThat(result.get(1).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(1).getTable()).isEqualTo("member");
            soft.assertThat(result.get(1).getDataRow().getLong("id")).isEqualTo(4L);
        });
    }

    private MariadbCdcConfig createConfig(String posFilePath) {
        return helper.createMariadbCdcConfigUsingConnectorFactory(posFilePath);
    }

}
