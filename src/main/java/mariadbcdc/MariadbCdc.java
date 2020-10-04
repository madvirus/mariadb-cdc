package mariadbcdc;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

public class MariadbCdc {
    private Logger logger = LoggerFactory.getLogger(MariadbCdc.class);

    private MariadbCdcListener listener = MariadbCdcListener.NO_OP;
    private MariadbCdcConfig config;
    private ColumnNamesGetter columnNamesGetter;
    private BinaryLogClient client;

    private ColumnNameCache columnNameCache;

    public MariadbCdc(MariadbCdcConfig config) {
        this(config, ColumnNamesGetter.NULL);
    }

    public MariadbCdc(MariadbCdcConfig config, ColumnNamesGetter columnNamesGetter) {
        this.config = config;
        this.columnNamesGetter = columnNamesGetter;
        this.columnNameCache = new ColumnNameCache(this.columnNamesGetter);
    }

    public void setMariadbCdcListener(MariadbCdcListener listener) {
        if (listener == null) this.listener = MariadbCdcListener.NO_OP;
        else this.listener = listener;
    }

    public void start() {
        BinaryLogClient client = new BinaryLogClient(
                config.getHost(), config.getPort(),
                config.getUser(), config.getPassword());

        createBinlogPositionFileIfNoExists();
        BinlogPosition lastBinPos = getSavedBinlogPosition();
        if (lastBinPos != null) {
            client.setBinlogFilename(lastBinPos.getFilename());
            client.setBinlogPosition(lastBinPos.getPosition());
        }
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                logger.info("mariadbCdc started : " + config.getUser() + "@" + config.getHost() + ":" + config.getPort());
                MariadbCdc.this.client = client;
                listener.started(new BinlogPosition(client.getBinlogFilename(), client.getBinlogPosition()));
            }
        });

        client.registerEventListener(createBinaryLogEventProcessor(client));

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    client.connect();
                } catch (Exception e) {
                    logger.error("mariadbCdc start failed : " + e.getMessage());
                    listener.startFailed(e);
                }
            }
        }).start();
    }

    private void createBinlogPositionFileIfNoExists() {
        Path path = Paths.get(config.getPositionTraceFile());
        if (Files.exists(path)) {
            return;
        }
        if (Files.isDirectory(path)) {
            throw new IllegalArgumentException(config.getPositionTraceFile() + " is directory path. so use tracefile.");
        }
        Path parentDir = path.toAbsolutePath().getParent();
        if (!Files.exists(parentDir)) {
            try {
                Files.createDirectories(parentDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            Files.write(path, Arrays.asList(""), StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new IllegalStateException("fail to create position trace file: " + e.getMessage(), e);
        }
    }

    private BinlogPosition getSavedBinlogPosition() {
        Path path = Paths.get(config.getPositionTraceFile());
        if (!Files.exists(path)) {
            return null;
        }
        try {
            List<String> lines = Files.readAllLines(path);
            for (String line : lines) {
                int idx = line.indexOf('/');
                if (idx > 0) {
                    return new BinlogPosition(line.substring(0, idx),
                            Long.parseLong(line.substring(idx + 1)));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("fail to read position trace file: " + e.getMessage(), e);
        }
        return null;
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
                new BinlogPositionSaver() {
                    @Override
                    public void save(BinlogPosition binPos) {
                        try {
                            Path path = Paths.get(config.getPositionTraceFile());
                            Files.write(path, Arrays.asList(binPos.getStringFormat()), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                            logger.debug("saved binary log position: {}", binPos.getStringFormat());
                        } catch (Exception ex) {
                            logger.error("fail to save binary log position: " + ex.getMessage(), ex);
                            throw new RuntimeException("fail to save binary log position: " + ex.getMessage(), ex);
                        }
                    }
                },
                this::getColumnNames,
                this::schemaChanged);
        processor.setIncludeFilters(config.getIncludeFilters());
        processor.setExcludeFilters(config.getExcludeFilters());
        return processor;
    }

    private List<String> getColumnNames(String database, String table) {
        return columnNameCache.getColumnNames(database, table);
    }

    private void schemaChanged(SchemaChangedData schemaChangedData) {
        columnNameCache.invalidate(schemaChangedData.getDatabase(), schemaChangedData.getTable());
    }

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
