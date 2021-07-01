package mariadbcdc;

import mariadbcdc.binlog.BinLogLifecycleListener;
import mariadbcdc.binlog.BinLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class BinLogReaderBinaryLogWrapper implements BinaryLogWrapper {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private MariadbCdcConfig config;
    private BinlogPosition lastBinPos;
    private MariadbCdcListener listener;
    private BinlogPositionSaver binlogPositionSaver;
    private ColumnNamesGetter columnNamesGetter;
    private SchemaChangeListener schemaChangeListener;

    private BinLogReader binLogReader;

    public BinLogReaderBinaryLogWrapper(MariadbCdcConfig config,
                                        BinlogPosition lastBinPos,
                                        MariadbCdcListener listener,
                                        BinlogPositionSaver binlogPositionSaver,
                                        ColumnNamesGetter columnNamesGetter,
                                        SchemaChangeListener schemaChangeListener) {
        this.config = config;
        this.lastBinPos = lastBinPos;
        this.listener = listener;
        this.binlogPositionSaver = binlogPositionSaver;
        this.columnNamesGetter = columnNamesGetter;
        this.schemaChangeListener = schemaChangeListener;
    }

    @Override
    public void start() {
        BinLogReader reader = new BinLogReader(
                config.getHost(), config.getPort(),
                config.getUser(), config.getPassword(),
                config.getHeartbeatPeriod()
        );
        if (config.getServerId() != null) {
            reader.setSlaveServerId(config.getServerId());
        }
        if (lastBinPos != null) {
            reader.setStartBinlogPosition(lastBinPos.getFilename(), lastBinPos.getPosition());
        }

        CountDownLatch latch = new CountDownLatch(1);

        reader.setBinLogListener(createBinLogListener(reader));
        reader.setBinLogLifecycleListener(new BinLogLifecycleListener() {
            @Override
            public void onStarted() {
                try {
                    BinlogPosition position = reader.getPosition();
                    listener.started(
                            new BinlogPosition(position.getFilename(), position.getPosition())
                    );
                } finally {
                    latch.countDown();
                }
            }
        });
        try {
            reader.connect();
            logger.info("BinLogReader connected : " + config.getUser() + "@" + config.getHost() + ":" + config.getPort());
        } catch (Exception e) {
            logger.error("BinLogReader start failed: " + e.getMessage());
            throw e;
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info("BinLogReader starting : " + config.getUser() + "@" + config.getHost() + ":" + config.getPort());
                    reader.start();
                } catch (Exception e) {
                    logger.error("BinLogReader start failed: " + e.getMessage());
                    try {
                        listener.startFailed(e);
                    } catch (Exception exception) {
                        // ignore callback exception
                    }
                } finally {
                    latch.countDown();
                }
            }
        }).start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (reader.isReading()) {
            this.binLogReader = reader;
        }
    }

    private BinLogReaderEventProcessor createBinLogListener(BinLogReader reader) {
        BinLogReaderEventProcessor processor = new BinLogReaderEventProcessor(
                listener,
                new CurrentBinlogFilenameGetter() {
                    @Override
                    public String getCurrentBinlogFilename() {
                        return reader.getBinlogFile();
                    }
                },
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangeListener
        );
        processor.setIncludeFilters(config.getIncludeFilters());
        processor.setExcludeFilters(config.getExcludeFilters());
        return processor;
    }

    @Override
    public boolean isStarted() {
        return this.binLogReader != null && this.binLogReader.isReading();
    }

    @Override
    public void stop() {
        if (binLogReader != null) {
            try {
                binLogReader.disconnect();
                logger.info("BinLogReader stopped : " + config.getUser() + "@" + config.getHost() + ":" + config.getPort());
                listener.stopped();
            } catch (Exception e) {
                throw new MariadbCdcStopFailException(e);
            }
        }
    }
}
