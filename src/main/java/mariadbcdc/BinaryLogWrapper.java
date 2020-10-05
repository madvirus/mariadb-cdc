package mariadbcdc;

public interface BinaryLogWrapper {
    void start();

    boolean isStarted();

    void stop();
}
