package mariadbcdc;

import mariadbcdc.shyikobinlog.DefaultBinaryLogWrapperFactory;
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

    private ColumnNameCache columnNameCache;

    private BinaryLogWrapperFactory wrapperFactory;
    private BinaryLogWrapper wrapper;

    public MariadbCdc(MariadbCdcConfig config) {
        this(config, ColumnNamesGetter.NULL);
    }

    public MariadbCdc(MariadbCdcConfig config, ColumnNamesGetter columnNamesGetter) {
        this.config = config;
        this.columnNamesGetter = columnNamesGetter;
        this.columnNameCache = new ColumnNameCache(this.columnNamesGetter);
        Class<? extends BinaryLogWrapperFactory> klass = config.getBinaryLogWrapperFactoryClass();
        if (klass != null) {
            try {
                wrapperFactory = klass.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            wrapperFactory = new DefaultBinaryLogWrapperFactory();
        }
    }

    public void setMariadbCdcListener(MariadbCdcListener listener) {
        if (listener == null) this.listener = MariadbCdcListener.NO_OP;
        else this.listener = listener;
    }

    public void start() {
        createBinlogPositionFileIfNoExists();
        BinlogPosition lastBinPos = getSavedBinlogPosition();

        BinaryLogWrapper wrapper = createWrapper(lastBinPos);
        wrapper.start();
        if (wrapper.isStarted()) {
            this.wrapper = wrapper;
        }
    }

    private BinaryLogWrapper createWrapper(BinlogPosition lastBinPos) {
        return wrapperFactory.create(config, lastBinPos,
                listener,
                this::saveBinlogPosition,
                this::getColumnNames,
                this::schemaChanged);
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

    private void saveBinlogPosition(BinlogPosition binPos) {
        try {
            Path path = Paths.get(config.getPositionTraceFile());
            Files.write(path, Arrays.asList(binPos.getStringFormat()), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            logger.debug("saved binary log position: {}", binPos.getStringFormat());
        } catch (Exception ex) {
            logger.error("fail to save binary log position: " + ex.getMessage(), ex);
            throw new RuntimeException("fail to save binary log position: " + ex.getMessage(), ex);
        }
    }

    private List<String> getColumnNames(String database, String table) {
        return columnNameCache.getColumnNames(database, table);
    }

    private void schemaChanged(SchemaChangedTable schemaChangedTable) {
        columnNameCache.invalidate(schemaChangedTable.getDatabase(), schemaChangedTable.getTable());
    }

    public void stop() {
        if (wrapper != null) {
            wrapper.stop();
        }
    }

}
