package mariadbcdc;

import java.util.Collections;
import java.util.List;

public class SchemaChangeQueryDecision {
    private boolean alterQuery;
    private List<SchemaChangedTable> schemaChangedTables;

    public SchemaChangeQueryDecision(boolean alterQuery, List<SchemaChangedTable> schemaChangedTables) {
        this.alterQuery = alterQuery;
        this.schemaChangedTables = Collections.unmodifiableList(schemaChangedTables);
    }

    public boolean isAlterQuery() {
        return alterQuery;
    }

    public List<SchemaChangedTable> getDatabaseTableNames() {
        return schemaChangedTables != null ? schemaChangedTables : Collections.emptyList();
    }
}
