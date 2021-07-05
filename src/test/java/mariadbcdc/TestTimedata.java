package mariadbcdc;

import java.sql.Date;
import java.sql.Timestamp;

public class TestTimedata {
    private Long id;
    private Timestamp dt;
    private Date da;

    public TestTimedata(Long id, Timestamp dt, Date da) {
        this.id = id;
        this.dt = dt;
        this.da = da;
    }

    public Long getId() {
        return id;
    }

    public Timestamp getDt() {
        return dt;
    }

    public Date getDa() {
        return da;
    }
}
