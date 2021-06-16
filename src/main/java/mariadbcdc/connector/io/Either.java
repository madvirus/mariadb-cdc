package mariadbcdc.connector.io;

public abstract class Either<L, R> {
    public boolean isLeft() { return false; }

    public L getLeft() { throw new IllegalStateException("not left"); }
    public boolean isRight() { return false; }
    public R getRight() { throw new IllegalStateException("not right"); }

    public static <L,R> Either<L,R> left(L value) {
        return new Left<L,R>(value);
    }

    public static <L,R> Either<L, R> right(R value) {
        return new Right<>(value);
    }

    public static class Left<L, R> extends Either<L, R> {
        private L value;

        public Left(L value) {
            this.value = value;
        }

        @Override
        public boolean isLeft() {
            return true;
        }

        @Override
        public L getLeft() {
            return value;
        }
    }

    public static class Right<L, R> extends Either<L, R> {
        private R value;

        public Right(R value) {
            this.value = value;
        }

        @Override
        public boolean isRight() {
            return true;
        }

        @Override
        public R getRight() {
            return value;
        }
    }
}
