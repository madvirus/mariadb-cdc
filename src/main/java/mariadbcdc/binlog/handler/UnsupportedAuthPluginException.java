package mariadbcdc.binlog.handler;

import mariadbcdc.binlog.BinLogException;

public class UnsupportedAuthPluginException extends BinLogException {
    public UnsupportedAuthPluginException(String authPluginName) {
        super("unsupported auth plugin: " + authPluginName);
    }
}
