package mariadbcdc;

public interface BinaryLogWrapperFactory {
    BinaryLogWrapper create(MariadbCdcConfig config,
                            BinlogPosition lastBinPos,
                            MariadbCdcListener listener,
                            BinlogPositionSaver binlogPositionSaver,
                            ColumnNamesGetter columnNamesGetter,
                            SchemaChangeListener schemaChangeListener);
}
