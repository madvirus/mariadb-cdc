package mariadbcdc.binlog.reader.handler;

import mariadbcdc.binlog.reader.BinLogException;

public class UnsupportedAuthPluginException extends BinLogException {
    public UnsupportedAuthPluginException(String authPluginName) {
        super("unsupported auth plugin: " + authPluginName);
    }
}
