package mariadbcdc;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@Testcontainers
public class JdbcColumnNamesGetterTest {
    @Container
    public MariaDBContainer mariaDB = (MariaDBContainer) new MariaDBContainer()
            .withConfigurationOverride("conf.d.103")
            .withInitScript("init.sql");

    @Test
    void getColumnNames() {
        JdbcColumnNamesGetter getter = new JdbcColumnNamesGetter(
                "localhost", mariaDB.getMappedPort(3306), "root", "test");
        List<String> cols = getter.getColumnNames("test", "member");
        assertThat(cols).contains("id", "name", "email", "use_yn", "reg");
    }
}
