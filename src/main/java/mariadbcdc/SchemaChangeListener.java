package mariadbcdc;

@FunctionalInterface
public interface SchemaChangeListener {

    void onSchemaChanged(SchemaChangedTable schemaChangedTable);

}
