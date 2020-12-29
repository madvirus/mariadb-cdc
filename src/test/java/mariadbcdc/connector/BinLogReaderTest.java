package mariadbcdc.connector;

import mariadbcdc.BinlogPosition;
import mariadbcdc.MariaCdcTestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BinLogReaderTest {
    @Test
    void todo() {
        MariaCdcTestHelper helper = new MariaCdcTestHelper("localhoset", 3306, "1", "root", "1");

        BinLogReader reader = new BinLogReader("localhost", 3306, "root", 1);
        reader.connect();
        BinlogPosition binlogPosition = reader.getPosition();
        reader.disconnect();

        BinlogPosition realPos = helper.getCurrentPosition();
        assertThat(binlogPosition.getFilename()).isEqualTo(realPos.getFilename());
        assertThat(binlogPosition.getPosition()).isEqualTo(realPos.getPosition());
    }
}
