package mariadbcdc.connector;

import mariadbcdc.BinlogPosition;
import mariadbcdc.MariaCdcTestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BinLogReaderTest {
    @Test
    void todo() {
        MariaCdcTestHelper helper = new MariaCdcTestHelper("localhost", 3306, "1", "root", "1");
        BinlogPosition realPos = helper.getCurrentPosition();

        BinLogReader reader = new BinLogReader("localhost", 3306, "root", "1");
        reader.connect();
        BinlogPosition binlogPosition = reader.getPosition();
        try {
            new Thread(() -> reader.start()).start();
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
            }
        } finally {
            reader.disconnect();
        }

        assertThat(binlogPosition.getFilename()).isEqualTo(realPos.getFilename());
        assertThat(binlogPosition.getPosition()).isEqualTo(realPos.getPosition());
    }
}
