package mariadbcdc.shyikobinlog;

import mariadbcdc.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class MariadbCdc_Time_Test {
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

    @Test
    void timedataTest() {
        helper.deleteTimedata();
        helper.deleteSavedPositionFile("temp/pos.file");
        MariadbCdcConfig config = createConfig("temp/pos.file");
        MariadbCdc cdc = new MariadbCdc(config,
                new JdbcColumnNamesGetter("localhost",
                        helper.port(), helper.cdcUser(), helper.cdcPassword()));

        List<RowChangedData> results = new ArrayList<>();
        cdc.setMariadbCdcListener(new MariadbCdcListener() {
            @Override
            public void onDataChanged(List<RowChangedData> list) {
                results.addAll(list);
            }
        });
        cdc.start();
        Sleeps.sleep(1);
        helper.insertTimedata(1L,
                LocalDateTime.of(2021, 7, 31, 19, 0, 3),
                LocalDate.of(2021, 7, 31));

        Sleeps.sleep(1);
        cdc.stop();

        LocalDateTime dt = results.get(0).getDataRow().getLocalDateTime("dt");
        assertThat(dt).isEqualTo(LocalDateTime.of(2021, 7, 31, 19, 0, 3));
    }

    private MariadbCdcConfig createConfig(String posFilePath) {
        return helper.createMariadbCdcConfig(posFilePath);
    }
}
