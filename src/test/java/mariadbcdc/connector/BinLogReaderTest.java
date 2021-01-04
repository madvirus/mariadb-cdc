package mariadbcdc.connector;

import mariadbcdc.BinlogPosition;
import mariadbcdc.MariaCdcTestHelper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class BinLogReaderTest {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    void todo() {
        MariaCdcTestHelper helper = new MariaCdcTestHelper("localhost", 3306, "root", "root", "root");
        BinlogPosition realPos = helper.getCurrentPosition();

        BinLogReader reader = new BinLogReader("localhost", 3306, "root", "root");
        reader.connect();
        BinlogPosition binlogPosition = reader.getPosition();
        try {
            new Thread(() -> reader.start()).start();
            try {
                Thread.sleep(60 * 1000);
            } catch (InterruptedException e) {
            }
        } finally {
            reader.disconnect();
        }

        assertThat(binlogPosition.getFilename()).isEqualTo(realPos.getFilename());
        assertThat(binlogPosition.getPosition()).isEqualTo(realPos.getPosition());
    }
}
