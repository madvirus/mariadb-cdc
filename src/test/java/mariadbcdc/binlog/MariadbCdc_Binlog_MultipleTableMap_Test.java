package mariadbcdc.binlog;

import mariadbcdc.*;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tag("integration")
@Testcontainers
public class MariadbCdc_Binlog_MultipleTableMap_Test {
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
    void multipleTableMap() throws SQLException {
        helper.deleteSavedPositionFile("temp/pos.file");
        MariadbCdcConfig config = helper.createMariadbCdcConfigUsingConnectorFactory("temp/pos.file");
        MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);

        helper.insertItem("ITEM1", "CODE1", "name1");
        helper.insertItemDetail("ITEM1", "CODE1", "description1");

        List<RowChangedData> result = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void onDataChanged(List<RowChangedData> list) {
                result.addAll(list);
            }
        });
        cdc.start();

        Sleeps.sleep(1);
        helper.updateItemCode("ITEM1", "CODE1-1");
        Sleeps.sleep(1);
        cdc.stop();

        Item item = helper.selectItem("ITEM1");
        ItemDetail itemDetail = helper.selectItemDetail("ITEM1");

        SoftAssertions.assertSoftly(s -> {
            s.assertThat(result).hasSize(1);
            s.assertThat(result.get(0).getTable()).isEqualTo("item");
            s.assertThat(result.get(0).getType()).isEqualTo(ChangeType.UPDATE);
            s.assertThat(result.get(0).getDataRow().getString("item_code")).isEqualTo("CODE1-1");
            s.assertThat(item.getCode()).isEqualTo("CODE1-1");
            s.assertThat(itemDetail.getCode()).isEqualTo("CODE1-1");
        });
    }

}
