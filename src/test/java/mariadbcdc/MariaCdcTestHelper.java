package mariadbcdc;

import mariadbcdc.binlog.BinLogReaderBinaryLogWrapperFactory;
import org.testcontainers.containers.MariaDBContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.stream.Collectors;

public class MariaCdcTestHelper {
    private String host;
    private Integer port;
    private String rootPassword;
    private String username;
    private String password;

    private String cdcUser;
    private String cdcPassword;

    public MariaCdcTestHelper(MariaDBContainer mariaDB) {
        host = "localhost";
        port = mariaDB.getMappedPort(3306);
        rootPassword = mariaDB.getPassword();
        username = mariaDB.getUsername();
        password = mariaDB.getPassword();
    }

    public MariaCdcTestHelper(String host, Integer port, String rootPassword, String username, String password) {
        this.host = host;
        this.port = port;
        this.rootPassword = rootPassword;
        this.username = username;
        this.password = password;
    }

    public void createCdcUser(String user, String password) {
        try (Connection conn = getRootConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + password + "'");
            stmt.executeUpdate("GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO '" + user + "'@'%'");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        this.cdcUser = user;
        this.cdcPassword = password;
    }

    public String cdcUser() {
        return cdcUser;
    }

    public String cdcPassword() {
        return cdcPassword;
    }

    public BinlogPosition getCurrentPosition() {
        try (Connection conn = getRootConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("show master status")
        ) {
            if (rs.next()) {
                return new BinlogPosition(rs.getString("File"), rs.getLong("Position"));
            }
            return null;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }



    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:mariadb://" + host + ":" + port() + "/test",
                getUsername(),
                getPassword());
    }

    private String getUsername() {
        return username;
    }

    public Connection getRootConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:mariadb://" + host + ":" + port(),
                "root",
                getRootPassword());
    }

    private String getRootPassword() {
        return rootPassword;
    }

    private String getPassword() {
        return password;
    }

    public int port() {
        return port;
    }

    public void changeMemberSchema() {
        try (Connection conn = getRootConnection();
             Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate("alter table test.member add column birthday date");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deleteSavedPositionFile(String positionFilePath) {
        Path path = Paths.get(positionFilePath);
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveCurrentPosition(String positionFilePath) {
        BinlogPosition binPos = getCurrentPosition();
        Path path = Paths.get(positionFilePath);
        try {
            Files.deleteIfExists(path);
            Files.write(path, Arrays.asList(binPos.getStringFormat()), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MariadbCdcConfig createMariadbCdcConfig(String posFilePath) {
        return new MariadbCdcConfig(
                host,
                port(),
                cdcUser(),
                cdcPassword(),
                posFilePath);
    }

    public MariadbCdcConfig createMariadbCdcConfigUsingConnectorFactory(String posFilePath) {
        MariadbCdcConfig config = createMariadbCdcConfig(posFilePath);
        config.setBinaryLogWrapperFactoryClass(BinLogReaderBinaryLogWrapperFactory.class);
        return config;
    }


    public void insertMember(String name, String email) {
        insertMember(name, email, LocalDateTime.now());
    }

    public void insertMember(String name, String email, LocalDateTime date) {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("insert into test.member (name, email, reg) values (?, ?, ?)")
        ) {
            pstmt.setString(1, name);
            pstmt.setString(2, email);
            pstmt.setTimestamp(3, Timestamp.valueOf(date));
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void insertMember(long id, String name) {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("insert into test.member (id, name) values (?, ?)")
        ) {
            pstmt.setLong(1, id);
            pstmt.setString(2, name);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void insertMemberWithBirth(String name, String email, LocalDate birth) {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("insert into test.member (name, email, birthday) values (?, ?, ?)")
        ) {
            pstmt.setString(1, name);
            pstmt.setString(2, email);
            pstmt.setDate(3, java.sql.Date.valueOf(birth));
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void updateMembers(TestPair<Long, String>... idNames) {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement("update test.member set name = ? where id = ?")) {
                for (TestPair<Long, String> idName : idNames) {
                    pstmt.setString(1, idName.val2);
                    pstmt.setLong(2, idName.val1);
                    pstmt.executeUpdate();
                }
            }
            conn.commit();
        } catch (SQLException ex) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex1) {
                }
            }
            throw new RuntimeException(ex);
        }
    }

    public void deleteMembers(Long... ids) {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            String cols = Arrays.stream(ids).map(l -> l.toString()).collect(Collectors.joining(","));
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("delete from test.member where id in ( " + cols + ")");
            }
            conn.commit();
        } catch (SQLException ex) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex1) {
                }
            }
            throw new RuntimeException(ex);
        }
    }

    public void insertUser(String loginId, String email) {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("insert into test.user (login_id, email) values (?, ?)")
        ) {
            pstmt.setString(1, loginId);
            pstmt.setString(2, email);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deleteUsers() {
        try (Connection conn = getConnection();
             Statement stmt1 = conn.createStatement()
        ) {
            stmt1.executeUpdate("delete from test.user");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void insertTimedata(Long id, LocalDateTime dt, LocalDate da) {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement("insert into test.timedata (id, dt, da) values (?, ?, ?)")
        ) {
            pstmt.setLong(1, id);
            pstmt.setTimestamp(2, Timestamp.valueOf(dt));
            pstmt.setDate(3, java.sql.Date.valueOf(da));
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public TestTimedata selectTimedata(Long id) {
        try (Connection conn = getConnection();
             Statement stmt1 = conn.createStatement();
             ResultSet rs = stmt1.executeQuery("select * from test.timedata where id = " + id)
        ) {
            if (!rs.next()) return null;
            return new TestTimedata(
                    rs.getLong("id"),
                    rs.getTimestamp("dt"),
                    rs.getDate("da")
            );
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deleteTimedata() {
        try (Connection conn = getConnection();
             Statement stmt1 = conn.createStatement()
        ) {
            stmt1.executeUpdate("truncate table test.timedata");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void runQuery(String query) {
        try (Connection conn = getConnection();
             Statement stmt1 = conn.createStatement()
        ) {
            stmt1.executeUpdate(query);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void truncateMember() {
        try (Connection conn = getConnection();
             Statement stmt1 = conn.createStatement()
        ) {
            stmt1.executeUpdate("truncate table test.member");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public MemberInserter withId(Long id) {
        return new MemberInserter().withId(id);
    }

    public class MemberInserter {
        private long id;
        private String name;
        private String email;
        private String useYn;
        private boolean agree;
        private String description;
        private LocalDate expdate;
        private LocalDateTime reg;

        public MemberInserter withId(long id) {
            this.id = id;
            return this;
        }

        public MemberInserter withName(String name) {
            this.name = name;
            return this;
        }

        public MemberInserter withEmail(String email) {
            this.email = email;
            return this;
        }

        public MemberInserter withUseYn(String useYn) {
            this.useYn = useYn;
            return this;
        }

        public MemberInserter withAgree(boolean agree) {
            this.agree = agree;
            return this;
        }

        public MemberInserter withDescription(String description) {
            this.description = description;
            return this;
        }

        public MemberInserter withExpdate(LocalDate expdate) {
            this.expdate = expdate;
            return this;
        }

        public MemberInserter withReg(LocalDateTime reg) {
            this.reg = reg;
            return this;
        }

        public void insert() {
            try (Connection conn = getConnection();
                 PreparedStatement pstmt = conn.prepareStatement("insert into test.member (id, name, email, use_yn, agree, description, expdate, reg) " +
                         "values (?, ?, ?, ?, ?, ?, ?, ?)")
            ) {
                pstmt.setLong(1, id);
                pstmt.setString(2, name);
                pstmt.setString(3, email);
                pstmt.setString(4, useYn);
                pstmt.setBoolean(5, agree);
                pstmt.setString(6, description);
                pstmt.setDate(7, expdate == null ? null : java.sql.Date.valueOf(expdate));
                pstmt.setTimestamp(8, reg == null ? null : Timestamp.valueOf(reg));
                pstmt.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }

        }
    }
}
