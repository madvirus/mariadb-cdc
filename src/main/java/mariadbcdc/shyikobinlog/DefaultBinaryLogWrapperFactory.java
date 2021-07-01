package mariadbcdc.shyikobinlog;

import mariadbcdc.*;

public class DefaultBinaryLogWrapperFactory implements BinaryLogWrapperFactory {
    @Override
    public BinaryLogWrapper create(MariadbCdcConfig config, BinlogPosition lastBinPos, MariadbCdcListener listener, BinlogPositionSaver binlogPositionSaver, ColumnNamesGetter columnNamesGetter, SchemaChangeListener schemaChangeListener) {
        BinaryLogWrapper wrapper = new DefaultBinaryLogWrapper(config, lastBinPos,
                listener,
                binlogPositionSaver,
                columnNamesGetter,
                schemaChangeListener);
        return wrapper;
    }
}
