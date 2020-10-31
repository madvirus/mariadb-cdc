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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class MariadbCdc_NoColumnNames_Test {
    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer()
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
    void testSet() {
        handleInsertedData();
    }

    void handleInsertedData() {
        helper.deleteSavedPositionFile("temp/pos.saved");
        helper.saveCurrentPosition("temp/pos.saved");

        helper.insertMember("name1", "email1");
        helper.changeMemberSchema();

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
        helper.insertMember("name2", "email2");
        Sleeps.sleep(1);
        cdc.stop();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result.get(0).getType()).isEqualTo(ChangeType.INSERT);
            soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(0).getTable()).isEqualTo("member");
            soft.assertThat(result.get(0).getDataRow().hasTableColumnNames()).isFalse();
            soft.assertThat(result.get(0).getDataRow().getColumnCount()).isEqualTo(3);
            soft.assertThat(result.get(0).getDataRow().getLong(0)).isNotNull();
            soft.assertThat(result.get(0).getDataRow().getString(1)).isEqualTo("name1");
            soft.assertThat(result.get(0).getDataRow().getString(2)).isEqualTo("email1");

            soft.assertThat(result.get(1).getType()).isEqualTo(ChangeType.INSERT);
            soft.assertThat(result.get(1).getDatabase()).isEqualTo("test");
            soft.assertThat(result.get(1).getTable()).isEqualTo("member");
            soft.assertThat(result.get(0).getDataRow().hasTableColumnNames()).isTrue();
            soft.assertThat(result.get(1).getDataRow().getLong("id")).isNotNull();
            soft.assertThat(result.get(1).getDataRow().getString("name")).isEqualTo("name2");
            soft.assertThat(result.get(1).getDataRow().getString("email")).isEqualTo("email2");
            soft.assertThat(result.get(1).getDataRow().getLocalDate("birthday")).isNull();
        });
    }
}
