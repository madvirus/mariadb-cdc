package mariadbcdc;

import java.util.Objects;

public class BinlogPosition {
    private String filename;
    private long position;

    public BinlogPosition(String filename, long position) {
        this.filename = filename;
        this.position = position;
    }

    public String getFilename() {
        return filename;
    }

    public long getPosition() {
        return position;
    }

    public String getStringFormat() {
        return filename + "/" + position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BinlogPosition that = (BinlogPosition) o;
        return position == that.position && Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, position);
    }

    @Override
    public String toString() {
        return "BinlogPosition{" +
                "filename='" + filename + '\'' +
                ", position=" + position +
                '}';
    }
}
