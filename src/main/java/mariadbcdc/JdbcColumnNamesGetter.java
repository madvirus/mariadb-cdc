package mariadbcdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JdbcColumnNamesGetter implements ColumnNamesGetter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final String host;
    private final int port;
    private final String user;
    private final String password;

    public JdbcColumnNamesGetter(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    @Override
    public List<String> getColumnNames(String database, String table) {
        try (Connection conn = DriverManager.getConnection("jdbc:mariadb://" + host + ":" + port, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "select COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE " +
                             "from INFORMATION_SCHEMA.COLUMNS " +
                             "WHERE table_schema = '" + database + "' and TABLE_NAME = '" + table + "' " +
                             "order by ORDINAL_POSITION")) {
            List<String> names = new ArrayList<>();
            if (rs.next()) {
                do {
                    names.add(rs.getString("COLUMN_NAME"));
                } while (rs.next());
                return names;
            } else {
                return Collections.emptyList();
            }
        } catch (SQLException ex) {
            logger.warn("fail to getColumnNames: " + ex.getMessage(), ex);
            return Collections.emptyList();
        }
    }

}
