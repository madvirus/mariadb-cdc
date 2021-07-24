package mariadbcdc;

public class Item {
    String item;
    String code;
    String name;

    public Item(String item, String code, String name) {
        this.item = item;
        this.code = code;
        this.name = name;
    }

    public String getItem() {
        return item;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
