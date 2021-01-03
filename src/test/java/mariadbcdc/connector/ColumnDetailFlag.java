package mariadbcdc.connector;

public enum ColumnDetailFlag {
    NOT_NULL(1), // field cannot be null
    PRIMARY_KEY(2), // field is a primary key
    UNIQUE_KEY(4), // field is unique
    MULTIPLE_KEY(8), // field is in a multiple key
    BLOB(16), // is this field a Blob
    UNSIGNED(32), // is this field unsigned
    ZEROFILL_FLAG(64), // is this field a zerofill
    BINARY_COLLATION(128), // whether this field has a binary collation
    ENUM(256), // Field is an enumeration
    AUTO_INCREMENT(512), // field auto-increment
    TIMESTAMP(1024), // field is a timestamp value
    SET(2048), // field is a SET
    NO_DEFAULT_VALUE_FLAG(4096), // field doesn't have default value
    ON_UPDATE_NOW_FLAG(8192), // field is set to NOW on UPDATE
    NUM_FLAG(32768), // field is num
    ;

    private int value;

    ColumnDetailFlag(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
