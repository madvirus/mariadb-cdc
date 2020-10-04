package mariadbcdc;

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
    public String toString() {
        return "BinlogPosition{" +
                "filename='" + filename + '\'' +
                ", position=" + position +
                '}';
    }
}
