package mariadbcdc.shyikobinlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import mariadbcdc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class DefaultBinaryLogWrapper implements BinaryLogWrapper {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final MariadbCdcConfig config;
    private final BinlogPosition lastBinPos;
    private MariadbCdcListener listener;
    private BinlogPositionSaver binlogPositionSaver;
    private ColumnNamesGetter columnNamesGetter;
    private SchemaChangeListener schemaChangeListener;

    private BinaryLogClient client;

    public DefaultBinaryLogWrapper(MariadbCdcConfig config,
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
        BinaryLogClient client = new BinaryLogClient(
                config.getHost(), config.getPort(),
                config.getUser(), config.getPassword());
        if (config.getServerId() != null) {
            client.setServerId(config.getServerId());
        }
        if (lastBinPos != null) {
            client.setBinlogFilename(lastBinPos.getFilename());
            client.setBinlogPosition(lastBinPos.getPosition());
        }
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(createBinaryLogEventProcessor(client));

        CountDownLatch latch = new CountDownLatch(1);
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                logger.info("mariadbCdc started : " + config.getUser() + "@" + config.getHost() + ":" + config.getPort());
                try {
                    listener.started(new BinlogPosition(client.getBinlogFilename(), client.getBinlogPosition()));
                } finally {
                    latch.countDown();
                }
            }
        });

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    client.connect();
                } catch (Exception e) {
                    logger.error("mariadbCdc start failed : " + e.getMessage());
                    try {
                        listener.startFailed(e);
                    } catch (Exception e2) {
                        // ignore startFailed callback exception
                    }
                    latch.countDown();
                }
            }
        }).start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (client.isConnected()) {
            this.client = client;
        }
    }

    private BinaryLogEventProcessor createBinaryLogEventProcessor(BinaryLogClient client) {
        BinaryLogEventProcessor processor = new BinaryLogEventProcessor(
                this.listener,
                new CurrentBinlogFilenameGetter() {
                    @Override
                    public String getCurrentBinlogFilename() {
                        return client.getBinlogFilename();
                    }
                },
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangeListener);
        processor.setIncludeFilters(config.getIncludeFilters());
        processor.setExcludeFilters(config.getExcludeFilters());
        return processor;
    }

    @Override
    public boolean isStarted() {
        return client != null && client.isConnected();
    }

    @Override
    public void stop() {
        if (client != null) {
            try {
                client.disconnect();
                logger.info("mariadbCdc stopped : " + config.getUser() + "@" + config.getHost() + ":" + config.getPort());
                this.listener.stopped();
            } catch (IOException e) {
                throw new MariadbCdcStopFailException(e);
            }
        }
    }
}
