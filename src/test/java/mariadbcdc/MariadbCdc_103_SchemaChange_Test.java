package mariadbcdc;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Tag("integration")
@Testcontainers
public class MariadbCdc_103_SchemaChange_Test {
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
        helper.insertMember("name1", "email1");
        helper.changeMemberSchema();
        helper.insertMemberWithBirth("name1", "email1", LocalDate.of(2020, 10, 3));
        Sleeps.sleep(1);

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(result).hasSize(2);
            if (result.size() == 2) {
                soft.assertThat(result.get(0).getType()).isEqualTo(ChangeType.INSERT);
                soft.assertThat(result.get(0).getDatabase()).isEqualTo("test");
                soft.assertThat(result.get(0).getTable()).isEqualTo("member");
                if (result.get(0).getDataRow().hasTableColumnNames()) {
                    soft.assertThat(result.get(0).getDataRow().getLong("id")).isEqualTo(1L);
                } else {
                    soft.assertThat(result.get(0).getDataRow().getLong("col1")).isEqualTo(1L);
                }

                soft.assertThat(result.get(1).getType()).isEqualTo(ChangeType.INSERT);
                soft.assertThat(result.get(1).getDatabase()).isEqualTo("test");
                soft.assertThat(result.get(1).getTable()).isEqualTo("member");
                soft.assertThat(result.get(1).getDataRow().getLong("id")).isEqualTo(2L);
                soft.assertThat(result.get(1).getDataRow().getLocalDate("birthday")).isEqualTo(LocalDate.of(2020, 10, 3));
            }
        });
    }
}
