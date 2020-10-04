package mariadbcdc;

public class TestPair<T,U> {
    public final T val1;
    public final U val2;

    public TestPair(T val1, U val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    public static <T, U> TestPair<T, U> of(T t, U u) {
        return new TestPair<>(t, u);
    }
}
