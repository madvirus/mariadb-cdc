package mariadbcdc;

public class UnsupportedTypeException extends RuntimeException {
    public UnsupportedTypeException(String message) {
        super(message);
    }
}
