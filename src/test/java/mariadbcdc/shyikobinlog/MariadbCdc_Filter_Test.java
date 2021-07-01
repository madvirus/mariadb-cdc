package mariadbcdc.shyikobinlog;

import mariadbcdc.*;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Tag("integration")
@Testcontainers
public class MariadbCdc_Filter_Test {
    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer("mariadb:10.3")
            .withConfigurationOverride("conf.d.103")
            .withInitScript("init.sql");

    private MariaCdcTestHelper helper;

    @BeforeEach
    void setUp() {
        helper = new MariaCdcTestHelper(mariaDB);
        helper.createCdcUser("cdc", "cdc");
    }

    private void prepareTest() {
        helper.deleteSavedPositionFile("temp/pos.file");
        helper.deleteUsers();
    }

    @Test
    void filterTestSet() {
        exclude();
        include();
    }

    private void exclude() {
        prepareTest();

        MariadbCdcConfig config = helper.createMariadbCdcConfig("temp/pos.file");
        config.setExcludeFilters("test.user");

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
        helper.insertMember("name1", "email1");
        Sleeps.sleep(1);
        helper.insertUser("user1", "name1");
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result).hasSize(1);
            soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(0).getTable()).isEqualTo("member");
        });
    }

    private void include() {
        prepareTest();

        MariadbCdcConfig config = helper.createMariadbCdcConfig("temp/pos.file");
        config.setIncludeFilters("test.user");

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
        helper.insertMember("name1", "email1");
        Sleeps.sleep(1);
        helper.insertUser("user2", "name2");
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result).hasSize(1);
            soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(0).getTable()).isEqualTo("user");
        });
    }
}
