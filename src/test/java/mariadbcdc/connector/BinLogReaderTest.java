package mariadbcdc.connector;

import mariadbcdc.BinlogPosition;
import mariadbcdc.MariaCdcTestHelper;
import mariadbcdc.TestPair;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;

public class BinLogReaderTest {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    void name() {
        byte[] a = new byte[] {(byte)0x3E};
        BitSet bitSet = BitSet.valueOf(a);
        System.out.println(bitSet.get(0));
        System.out.println(bitSet);
    }

    @Test
    void todo() throws InterruptedException {
        MariaCdcTestHelper helper = new MariaCdcTestHelper("localhost", 3306, "root", "root", "root");
        helper.truncateMember();
        helper.insertMember(1L, "name");
        BinlogPosition realPos = helper.getCurrentPosition();

        BinLogReader reader = new BinLogReader("localhost", 3306, "root", "root");
        reader.connect();
        BinlogPosition binlogPosition = reader.getPosition();

        runAsync(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }
            try {
                String post = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                helper.updateMembers(TestPair.of(1L, "name" + post));
                logger.info("update Members");
            } catch (Exception e) {
                logger.error("fail to update Members: ", e);
            }
        });
        try {
            runAsync(() -> reader.start());
            try {
                Thread.sleep(60 * 1000);
            } catch (InterruptedException e) {
            }
        } finally {
            reader.disconnect();
        }

        assertThat(binlogPosition.getFilename()).isEqualTo(realPos.getFilename());
        assertThat(binlogPosition.getPosition()).isEqualTo(realPos.getPosition());
        System.out.println("한글 출력!");
    }

    private void runAsync(Runnable runnable) {
        new Thread(runnable).start();
    }
}
