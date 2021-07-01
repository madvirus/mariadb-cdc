package mariadbcdc.binlog;

import mariadbcdc.*;

public class BinLogReaderBinaryLogWrapperFactory implements BinaryLogWrapperFactory {
    @Override
    public BinaryLogWrapper create(MariadbCdcConfig config, BinlogPosition lastBinPos, MariadbCdcListener listener, BinlogPositionSaver binlogPositionSaver, ColumnNamesGetter columnNamesGetter, SchemaChangeListener schemaChangeListener) {
        return new BinLogReaderBinaryLogWrapper(
                config,
                lastBinPos,
                listener,
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangeListener
        );
    }
}
