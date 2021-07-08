# Usage

## MariaDB Configuration

### Enabling binary logging: binlog_format setting
To enable binary logging configure the following settings on the master server and restart the service:

```
binlog_format = row
binlog_row_image = full
```

> If binlog_row_image is full, all columns in the before and after image are logged.

### Creating a user for cdc
To use cdc, create a user which is granted REPLICATION SLAVE, REPLICATION CLIENT and SELECT privileges.

```
CREATE USER cdc@'%' IDENTIFIED BY 'password'
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO cdc@'%'
```

## Repository

### Maven
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

```xml
<dependency>
    <groupId>com.github.madvirus</groupId>
    <artifactId>mariadb-cdc</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle
```
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.madvirus:mariadb-cdc:1.0.0'
}
```

## Using MariadbCdc

### Basic usage

#### Step 1, create a MariadbCdcConfig

Create a MariadbCdcConfig:
```java
MariadbCdcConfig config = new MariadbCdcConfig(
                "localhost", // host
                3306, // port
                "cdc", // user for cdc
                "password", // password
                "bin.pos"); // bin position trace file
```

If no "bin.pos" file exists, read from current binary log position.
If "bin.pos" file contains binary log position, read from that position.
MariadbCdc records next position while reading binary log.

#### Step 2, create a MariadbCdc

```java
JdbcColumnNamesGetter columnNamesGetter = new JdbcColumnNamesGetter(
            "localhost", // host
            3307, // port
            "cdc", // cdc user
            "password"); // password 

MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);
```

MariadbCdc uses a ColumnNamesGetter to extract column names of table. 

JdbcColumnNamesGetter use the following query to get column names:
```sql
select COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE 
from INFORMATION_SCHEMA.COLUMNS 
WHERE table_schema = '?' and TABLE_NAME = '?' 
order by ORDINAL_POSITION
``` 

If you use mariadb 10.5 or above version and config 'full' binlog_row_metadata value,
then JdbcColumnNamesGetter is not needed to get column names:

```
// use mariadb 10.5 and 'full' binlog_row_metadata value
// JdbcColumnNamesGetter is not needed
MariadbCdc cdc = new MariadbCdc(config);
```

#### Step 3, set MariadbCdcListener

Use MariadbCdc#setMariadbCdcListener method to handle CDC event:

```
cdc.setMariadbCdcListener(new MariadbCdcListener() {
    @Override
    public void started(BinlogPosition binlogPosition) {
        // cdc started
    }

    @Override
    public void startFailed(Exception e) {
        // failed to start cdc
    }

    @Override
    public void onDataChanged(List<RowChangedData> list) {
        // handle changed data
        list.forEach(data -> { // each
            String database = data.getDatabase(); // get database name of changed row
            String table = data.getTable(); // get table name of changed row
            DataRow dataRow = data.getDataRow(); // get changed row data
            if (data.getType() == ChangeType.INSERT) {
                Long id = dataRow.getLong("id"); // get Long value of id column
                // ...
            } else if (data.getType() == ChangeType.UPDATE) {
                String name = dataRow.getString("name"); // get String value of updated name column
                DataRow dataRowBeforeUpdate = data.getDataRowBeforeUpdate(); // before image
                String nameBeforeUpdate = dataRowBeforeUpdate.getString("name"); // get String value of name column before update
                // ...
            } else if (data.getType() == ChangeType.DELETE) {
                String email = dataRow.getString("email"); // get value of email column of deleted row
                // ...
            }
        });
    }

    @Override
    public void onXid(Long xid) {
        // transaction commit log 
    }

    @Override
    public void stopped() {
        // cdc stopped
    }
});
```

#### Step 4, start/stop MariadbCdc

```java
// start reading binary log.
// start() method launches a separate thread for reading log 
cdc.start(); // do not block current thread

...

cdc.stop(); // stop reading
```

> MariadbCdc#start() method launches a separate thread for reading binary log,
so this method does not block the current thread.

### Including/Excluding specific tables

By default, MariadbCdcListener#onDataChanged() is called for every changes.
If you want to include/exclude specific tables, use filters:   

```java
// onDataChanged() called when only test.user table is changed
config.setIncludeFilters("test.user");
```

```java
// onDataChanged() is not called when test.member table is changed
config.setIncludeFilters("test.member");
```

### Includes only updated columns

To include only updated columns, set binlog_row_image to minimal:

```
binlog_format = row
binlog_row_image = minimal
``` 

> MINIMAL means that a PK equivalent (PK columns or full row if there is no PK in the table) is logged in the before image, and only changed columns are logged in the after image

When binlog_row_image is minimal and run the following query:
```sql
update member set name = 'newname' where id = 10
```

then RowChangedData#getDataRowBeforeUpdate() returns a DataRow which contains only pk columns,
and RowChangedData#getDataRow() returns a DataRow which contains only updated columns.

```java
@Override
public void onDataChanged(List<RowChangedData> list) {
    // handle changed data
    list.forEach(data -> { // each
        String database = data.getDatabase(); // test
        String table = data.getTable(); // member
        DataRow afterDataRow = data.getDataRow(); // after image
        if (data.getType() == ChangeType.UPDATE) {
            DataRow beforeDataRow = data.getDataRowBeforeUpdate(); // before image
            Long id = beforeDataRow.getLong("id"); // before image includes only pk columns
            String name = afterDataRow.getString("name"); // after image includes only updated columns
            // ...
        }
    });
}
```

### MariaDB 10.5: binlog_row_metadata = full

MariaDB 10.5 supports binlog_row_metadata config variable.
When binlog_row_metadata is FULL, then all metadata (including column names) is logged.
So if binlog_row_metadata is FULL, no ColumnNamesGetter is required.

## Binlog reader implementation

Supported Two binlog readers:
* DefaultBinaryLogWrapper (default)
* BinLogReaderBinaryLogWrapper (experiment)

Default binlog reader is DefaultBinaryLogWrapper.
DefaultBinaryLogWrapper use a mysql-binlog-connector-java(shyiko binlog).

If you want to change reader, use MariadbCdcConfig#setBinaryLogWrapperFactoryClass method: 

```
config.setBinaryLogWrapperFactoryClass(BinLogReaderBinaryLogWrapperFactory.class);
```

### BinLogReaderBinaryLogWrapper contraints

* Only username/password authentication support
* No SSL support
* No gtid support