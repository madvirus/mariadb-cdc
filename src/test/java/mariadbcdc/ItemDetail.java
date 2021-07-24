package mariadbcdc;

public class ItemDetail {
    String item;
    String code;
    String description;

    public ItemDetail(String item, String code, String description) {
        this.item = item;
        this.code = code;
        this.description = description;
    }

    public String getItem() {
        return item;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
