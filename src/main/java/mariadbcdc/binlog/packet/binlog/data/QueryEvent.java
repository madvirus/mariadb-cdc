package mariadbcdc.binlog.packet.binlog.data;

import mariadbcdc.binlog.packet.binlog.BinLogData;

import java.util.Arrays;

public class QueryEvent implements BinLogData {

    /** uint<4> The ID of the thread that issued this statement on the master. */
    private long threadId;
    /** uint<4> The time in seconds that the statement took to execute. */
    private long executeTime;
    /** uint<1> The length of the name of the database which was the default database when the statement was executed. This name appears later, in the variable data part. It is necessary for statements such as INSERT INTO t VALUES(1) that don't specify the database and rely on the default database previously selected by USE. */
    private int lengthOfDatabaseName;
    /** uint<2> The error code resulting from execution of the statement on the master. */
    private int errorCode;
    /** uint<2> The length of the status variable block. */
    private int lengthOfVariableBlock;
    private byte[] statusVariables;
    private String defaultDatabase;
    private String sql;

    public QueryEvent(long threadId,
                      long executeTime,
                      int lengthOfDatabaseName,
                      int errorCode,
                      int lengthOfVariableBlock,
                      byte[] statusVariables,
                      String defaultDatabase,
                      String sql) {
        this.threadId = threadId;
        this.executeTime = executeTime;
        this.lengthOfDatabaseName = lengthOfDatabaseName;
        this.errorCode = errorCode;
        this.lengthOfVariableBlock = lengthOfVariableBlock;
        this.statusVariables = statusVariables;
        this.defaultDatabase = defaultDatabase;
        this.sql = sql;
    }

    public long getThreadId() {
        return threadId;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public int getLengthOfDatabaseName() {
        return lengthOfDatabaseName;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getLengthOfVariableBlock() {
        return lengthOfVariableBlock;
    }

    public byte[] getStatusVariables() {
        return statusVariables;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public String toString() {
        return "QueryEvent{" +
                "threadId=" + threadId +
                ", executeTime=" + executeTime +
                ", lengthOfDatabaseName=" + lengthOfDatabaseName +
                ", errorCode=" + errorCode +
                ", lengthOfVariableBlock=" + lengthOfVariableBlock +
                ", statusVariables=" + Arrays.toString(statusVariables) +
                ", defaultDatabase='" + defaultDatabase + '\'' +
                ", sql='" + sql + '\'' +
                '}';
    }
}
