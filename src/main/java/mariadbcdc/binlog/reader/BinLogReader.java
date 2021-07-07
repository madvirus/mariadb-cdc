package mariadbcdc.binlog.reader;

import mariadbcdc.BinlogPosition;
import mariadbcdc.binlog.reader.io.Either;
import mariadbcdc.binlog.reader.packet.ErrPacket;
import mariadbcdc.binlog.reader.packet.binlog.BinLogEvent;
import mariadbcdc.binlog.reader.packet.binlog.BinlogEventType;
import mariadbcdc.binlog.reader.packet.binlog.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BinLogReader {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private String readerId;

    private ConnectionInfo connectionInfo;
    private Duration heartbeatPeriod;

    private BinLogSession session;
    private AtomicBoolean disconnected = new AtomicBoolean(true);
    private AtomicBoolean reading = new AtomicBoolean(false);

    private BinLogListener listener = BinLogListener.NULL;
    private BinLogLifecycleListener binLogLifecycleListener = BinLogLifecycleListener.NULL;

    private String binlogFile;
    private long binlogPosition;
    private long slaveServerId = 65535;

    private AtomicLong lastEventTimestamp = new AtomicLong(0L);

    private boolean reconnection = false;
    private Duration keepConnectionTimeout;
    private ReconnectThread reconnectThread;

    public BinLogReader(String host, int port, String user, String password) {
        this(host, port, user, password, null);
    }

    public BinLogReader(String host, int port, String user, String password, Duration heartbeatPeriod) {
        assignReaderIdRandomly();
        connectionInfo = new ConnectionInfo(host, port, user, password);
        this.heartbeatPeriod = heartbeatPeriod;
    }

    private void assignReaderIdRandomly() {
        this.readerId = Integer.toString(ThreadLocalRandom.current().nextInt(1000, 10000));
    }

    public void setStartBinlogPosition(String filename, long position) {
        this.binlogFile = filename;
        this.binlogPosition = position;
    }

    public void setSlaveServerId(long slaveServerId) {
        this.slaveServerId = slaveServerId;
    }

    public void setBinLogListener(BinLogListener listener) {
        if (listener != null) {
            this.listener = listener;
        } else {
            this.listener = BinLogListener.NULL;
        }
    }

    public void setBinLogLifecycleListener(BinLogLifecycleListener binLogLifecycleListener) {
        if (binLogLifecycleListener != null) {
            this.binLogLifecycleListener = binLogLifecycleListener;
        } else {
            this.binLogLifecycleListener = BinLogLifecycleListener.NULL;
        }
    }

    public void connect() {
        int trycnt = 1;
        while(true) {
            try {
                session = new BinLogSession(connectionInfo);
                session.handshake();

                if (heartbeatPeriod != null && !heartbeatPeriod.isNegative() && heartbeatPeriod.getSeconds() > 0) {
                    session.enableHeartbeat(heartbeatPeriod);
                }
                if (binlogFile == null) {
                    BinlogPosition pos = session.fetchBinlogFilePosition();
                    this.binlogFile = pos.getFilename();
                    this.binlogPosition = pos.getPosition();
                }
                break;
            } catch (Exception ex) {
                if (session != null) {
                    session.close();
                    session = null;
                }
                if (trycnt < 3) {
                    trycnt++;
                } else {
                    throw ex;
                }
            }
        }
        logger.debug("[readerId={}] connected", readerId);
        binLogLifecycleListener.onConnected();
    }

    public void start() {
        registerSlave();
        startReconnectThreadIfReconnectionEnabled();
        while(isKeepReading()) {
            try {
                read();
            } catch (BinLogException e) {
                if (reading.get()) {
                    throw e;
                }
                logger.debug("[readerId={}] slaveServerId={} ignore BinLogException because disconnected", readerId, slaveServerId);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                }
            }
        }
    }

    private Lock lock = new ReentrantLock();

    private boolean isKeepReading() {
        lock.lock();
        try {
            return reconnection && !disconnected.get();
        } finally {
            lock.unlock();
        }
    }

    private void registerSlave() {
        session.registerSlave(binlogFile, binlogPosition, slaveServerId);
        logger.debug("[readerId={}] slaveServerId={} slave registered", readerId, slaveServerId);
        this.disconnected.set(false);
        updateLastEventTimestamp();
        binLogLifecycleListener.onStarted();
    }

    private void updateLastEventTimestamp() {
        lastEventTimestamp.set(System.currentTimeMillis());
    }

    private void startReconnectThreadIfReconnectionEnabled() {
        if (reconnection) {
            reconnectThread = new ReconnectThread();
            reconnectThread.start();
            logger.debug("[readerId={}] slaveServerId={} start ReconnectThread", readerId, slaveServerId);
        }
    }

    private void read() {
        if (session == null) return;
        reading.set(true);
        try {
            while (reading.get()) {
                Either<ErrPacket, BinLogEvent> readEvent = session.readBinlog();
                updateLastEventTimestamp();
                try {
                    if (readEvent.isLeft()) {
                        ErrPacket errPacket = readEvent.getLeft();
                        logger.debug("[readerId={}] slaveServerId={} ErrPacket: {}", readerId, slaveServerId, errPacket);
                        listener.onErr(errPacket);
                    } else {
                        BinLogEvent event = readEvent.getRight();

                        if (event.getHeader().getEventType() == BinlogEventType.ROTATE_EVENT) {
                            RotateEvent rotateEvent = (RotateEvent) event.getData();
                            this.binlogFile = rotateEvent.getFilename();
                            this.binlogPosition = rotateEvent.getPosition();
                            listener.onRotateEvent(event.getHeader(), rotateEvent);
                        } else {
                            this.binlogPosition = event.getHeader().getNextPosition();
                            if (event.getHeader().getEventType() == BinlogEventType.FORMAT_DESCRIPTION_EVENT) {
                                listener.onFormatDescriptionEvent(event.getHeader(), (FormatDescriptionEvent) event.getData());
                            } else if (event.getHeader().getEventType() == BinlogEventType.QUERY_EVENT) {
                                listener.onQueryEvent(event.getHeader(), (QueryEvent) event.getData());
                            } else if (event.getHeader().getEventType() == BinlogEventType.TABLE_MAP_EVENT) {
                                listener.onTableMapEvent(event.getHeader(), (TableMapEvent) event.getData());
                            } else if (event.getHeader().getEventType() == BinlogEventType.WRITE_ROWS_EVENT_V1) {
                                listener.onWriteRowsEvent(event.getHeader(), (WriteRowsEvent) event.getData());
                            } else if (event.getHeader().getEventType() == BinlogEventType.UPDATE_ROWS_EVENT_V1) {
                                listener.onUpdateRowsEvent(event.getHeader(), (UpdateRowsEvent) event.getData());
                            } else if (event.getHeader().getEventType() == BinlogEventType.DELETE_ROWS_EVENT_V1) {
                                listener.onDeleteRowsEvent(event.getHeader(), (DeleteRowsEvent) event.getData());
                            } else if (event.getHeader().getEventType() == BinlogEventType.XID_EVENT) {
                                listener.onXidEvent(event.getHeader(), (XidEvent) event.getData());
                            } else if (event.getHeader().getEventType() == BinlogEventType.HEARTBEAT_LOG_EVENT) {
                                listener.onHeartbeatEvent(event.getHeader(), (HeartbeatEvent) event.getData());
                            } else if (event.getHeader().getEventType() == BinlogEventType.STOP_EVENT) {
                                reading.set(false);
                                listener.onStopEvent(event.getHeader(), (StopEvent) event.getData());
                            }
                        }
                    }
                } catch (Exception ex) {
                    logger.warn("[readerId={}] slaveServerId={} ignore exception: {}", readerId, slaveServerId, ex.getMessage());
                }
            }
        } finally {
            reading.set(false);
        }
    }

    public BinlogPosition getPosition() {
        return new BinlogPosition(binlogFile, binlogPosition);
    }

    public String getBinlogFile() {
        return binlogFile;
    }

    public void disconnect() {
        disconnected.set(true);
        reading.compareAndSet(true, false);
        stopReconnectThread();
        closeSession();
        logger.debug("[readerId={}] slaveServerId={} disconnected", readerId, slaveServerId);
    }

    private void closeSession() {
        if (session != null) {
            session.close();
            logger.debug("[readerId={}] slaveServerId={} closed session", readerId, slaveServerId);
            try {
                binLogLifecycleListener.onDisconnected();
            } catch (Exception e) {
                // ignore
            }
        }
        session = null;
    }

    private void stopReconnectThread() {
        if (reconnectThread != null) {
            reconnectThread.stopReconnect();
            logger.warn("[readerId={}] slaveServerId={} stop ReconnectThread", readerId, slaveServerId);
            reconnectThread = null;
        }
    }

    public boolean isReading() {
        return reading.get();
    }

    public void enableReconnection() {
        this.reconnection = true;
    }

    /**
     * Max time to keep connection without no event receiving.
     * This value is used only if reconnection is enabled.
     * BinLogReader check last event received time and reconnect to server if no event received in keepConnectionTimeout
     *
     * This valud must be greater than heartbeatPeriod.
     *
     * @param keepConnectionTimeout
     */
    public void setKeepConnectionTimeout(Duration keepConnectionTimeout) {
        this.keepConnectionTimeout = keepConnectionTimeout;
    }

    public void setHeartbeatPeriod(Duration heartbeatPeriod) {
        this.heartbeatPeriod = heartbeatPeriod;
    }

    public void setReaderId(String readerId) {
        this.readerId = readerId;
    }

    class ReconnectThread extends Thread {
        private boolean reconnectRunning = true;
        public ReconnectThread() {
        }

        @Override
        public void run() {
            reconnectRunning = true;
            while(reconnectRunning) {
                try {
                    Thread.sleep(keepConnectionTimeout.toMillis());
                    if (System.currentTimeMillis() - lastEventTimestamp.get() > keepConnectionTimeout.toMillis()) {
                        logger.warn("[readerId={}] slaveServerId={} no event received in {}, so try reconnect", readerId, slaveServerId, keepConnectionTimeout);
                        tryReconnect();
                    }
                } catch (InterruptedException e) {
                }
            }
        }

        public void stopReconnect() {
            reconnectRunning = false;
            this.interrupt();
        }
    }

    private void tryReconnect() {
        lock.lock();
        try {
            closeSession();
            try {
                connect();
            } catch (Exception e) {
                logger.warn("[readerId={}] slaveServerId={} fail to reconnect: {}", readerId, slaveServerId, e.getMessage());
                return;
            }
            try {
                registerSlave();
                logger.debug("[readerId={}] slaveServerId={} reconnected", readerId, slaveServerId);
            } catch (Exception e) {
                logger.warn("[readerId={}] slaveServerId={} fail to reregister slave: {}", readerId, slaveServerId, e.getMessage());
                closeSession();
            }
        } finally {
            lock.unlock();
        }
    }
}
