package mariadbcdc.connector.handler;

import mariadbcdc.connector.BinLogException;

public class UnsupportedAuthPluginException extends BinLogException {
    public UnsupportedAuthPluginException(String authPluginName) {
        super("unsupported auth plugin: " + authPluginName);
    }
}
