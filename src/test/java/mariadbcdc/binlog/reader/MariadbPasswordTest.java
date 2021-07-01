package mariadbcdc.binlog.reader;

import mariadbcdc.binlog.reader.MariadbPassword;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.internal.util.Utils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;

class MariadbPasswordTest {

    @Test
    void name() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        byte[] encBytes = MariadbPassword.nativePassword("1", "-?76GE`70)|X$Ft8Y5?{");
        System.out.println(new BigInteger(1, encBytes).toString(16));

        byte[] encBytes2 = Utils.encryptPassword(
                "1", "-?76GE`70)|X$Ft8Y5?{".getBytes(),
                "utf-8");

        System.out.println(new BigInteger(1, encBytes2).toString(16));
    }

}