package mariadbcdc.connector;

import java.security.MessageDigest;

public class MariadbPassword {

    public static byte[] nativePassword(String password, String seedStr) {
        return nativePassword(password, seedStr.getBytes());
    }

    public static byte[] nativePassword(String password, byte[] seed) {
        if (password == null || password.isEmpty()) {
            return new byte[0];
        }
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
            byte[] bytePwd = password.getBytes("utf-8");
            final byte[] stage1 = messageDigest.digest(bytePwd);
            messageDigest.reset();
            final byte[] stage2 = messageDigest.digest(stage1);
            messageDigest.reset();
            messageDigest.update(seed);
            messageDigest.update(stage2);
            final byte[] digest = messageDigest.digest();
            final byte[] returnBytes = new byte[digest.length];
            for (int i = 0; i < digest.length; i++) {
                returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
            }
            return returnBytes;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
