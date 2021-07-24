# 사용법

## MariaDB 설정

### 바이너리 로그 사용: binlog_format 설정
바이너리 로그를 사용하려면 마스터 서버에 다음 설정을 추가하고 재시작한다:

```
binlog_format = row
binlog_row_image = full
```

> binlog_row_image 설정이 full이면 모든 칼럼 값을 기록한다.

### cdc 용 사용자 생성
REPLICATION SLAVE, REPLICATION CLIENT, SELECT 권한을 가진 사용자를 CDC 용도로 생성한다.

```
CREATE USER cdc@'%' IDENTIFIED BY 'password'
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO cdc@'%'
```

## 리포지토리

### 메이븐
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
    <version>1.0.2</version>
</dependency>
```

### 그레이들
```
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.madvirus:mariadb-cdc:1.0.2'
}
```

## MariadbCdc 사용하기

### 기본 사용법

#### 단계 1, MariadbCdcConfig 생성

MariadbCdcConfig를 생성한다:
```java
MariadbCdcConfig config = new MariadbCdcConfig(
                "localhost", // 호스트
                3306, // 포트
                "cdc", // CDC용 사용자
                "password", // 암호
                "bin.pos"); // 바이너리 위치 추적 파일
```

"bix.pos" 파일이 없으면 현재 바이너리 로그 위치부터 읽어온다.
"bix.pos" 파일이 바이너리 로그 위치를 포함하면 그 위치부터 읽어온다.
MariadbCdc는 바이너리 로그를 읽는 동안 다음 위치를 파일에 기록한다.

#### 단계 2, MariadbCdc 생성

```java
JdbcColumnNamesGetter columnNamesGetter = new JdbcColumnNamesGetter(
            "localhost", // host
            3307, // port
            "cdc", // cdc user
            "password"); // password 

MariadbCdc cdc = new MariadbCdc(config, columnNamesGetter);
```

MariadbCdc는 테이블의 칼럼 이름을 추출하기 위해 ColumnNamesGetter을 사용한다.

JdbcColumnNamesGetter는 칼럼 이름을 구하기 위해 다음 쿼리를 사용한다:
```sql
select COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE 
from INFORMATION_SCHEMA.COLUMNS 
WHERE table_schema = '?' and TABLE_NAME = '?' 
order by ORDINAL_POSITION
``` 

마리아DB 10.5나 그 이상을 사용하고 binlog_row_metadata 값으로 'full'을 사용하면
JdbcColumnNamesGetter가 필요 없다:

```
// 마리아DB 1.5를 사용하고 binlog_row_metadata 값이 'full'이면
// JdbcColumnNamesGetter가 필요 없다
MariadbCdc cdc = new MariadbCdc(config);
```

#### 단계 3, MariadbCdcListener 설정

CDC 이벤트를 처리할 MariadbCdcListener를 생성하고 MariadbCdc#setMariadbCdcListener로 설정:

```
cdc.setMariadbCdcListener(new MariadbCdcListener() {
    @Override
    public void started(BinlogPosition binlogPosition) {
        // cdc 시작함
    }

    @Override
    public void startFailed(Exception e) {
        // cdc 시작에 실패함
    }

    @Override
    public void onDataChanged(List<RowChangedData> list) {
        // 데이터 변경 처리
        list.forEach(data -> { // 각 변경 데이터
            String database = data.getDatabase(); // 변경된 행의 DB 이름
            String table = data.getTable(); // 변경된 행의 테이블 이름
            DataRow dataRow = data.getDataRow(); // 변경된 데이터 구함
            if (data.getType() == ChangeType.INSERT) {
                Long id = dataRow.getLong("id"); // id 칼럼을 Long 값으로 구함
                // ...
            } else if (data.getType() == ChangeType.UPDATE) {
                String name = dataRow.getString("name"); // 변경된 name 칼럼을 String 값으로 구함
                DataRow dataRowBeforeUpdate = data.getDataRowBeforeUpdate(); // 변경 전 이미지(데이터)
                String nameBeforeUpdate = dataRowBeforeUpdate.getString("name"); // 변경 전 name 칼럼 값을 String 값으로 구함
                // ...
            } else if (data.getType() == ChangeType.DELETE) {
                String email = dataRow.getString("email"); // 삭제된 행의 email 칼럼 값을 구함
                // ...
            }
        });
    }

    @Override
    public void onXid(Long xid) {
        // 트랜잭션 커밋 로그 
    }

    @Override
    public void stopped() {
        // cdc 멈춤
    }
});
```

#### 단계 4, MariadbCdc 시작/중지하기

```java
// 바이너리 로그 읽기 시작
// start() 메서드는 로그를 읽기 위해 별도 쓰레드를 사용한다. 
cdc.start(); // 현재 쓰레드를 블로킹하지 않음

...

cdc.stop(); // 읽기 멈춤
```

> MariadbCdc#start() 메서드는 바이너리 로그를 읽기 위해 별도 쓰레드를 사용한다.
그래서 이 메서드는 현재 쓰레드를 블로킹하지 않는다.

### 특정 테이블 포함/예외 처리

기본으로 모든 변경에 대해 MariadbCdcListener#onDataChanged() 메서드를 호출한다.
특정 테이블만 포함하거나 특정 테이블을 제외하고 싶으면 필터를 사용하면 된다:

```java
// test.user 테이블이 변경될 때만 onDataChanged() 메서드를 호출
config.setIncludeFilters("test.user");
```

```java
// test.member 테이블이 바껴도 onDataChanged() 메서드를 호출하지 않음
config.setIncludeFilters("test.member");
```

### 변경된 칼럼만 포함하기

변경된 칼럼만 포함하고 싶다면 binlog_row_image를 minimal로 설정한다:

```
binlog_format = row
binlog_row_image = minimal
``` 

> MINIMAL은 변경전 이미지에는 PK만 포함하고(PK가 없으면 전체 칼럼) 변경후 이미지에는 변경된 칼럼만 기록한다  

binlog_row_image가 minimal일 때 다음 쿼리를 실행하면:
```sql
update member set name = 'newname' where id = 10
```

RowChangedData#getDataRowBeforeUpdate()는 PK 칼럼만 포함한 DataRow를 리턴하고
RowChangedData#getDataRow()는 변경된 칼럼만 포함한 DataRow를 리턴한다.

```java
@Override
public void onDataChanged(List<RowChangedData> list) {
    // 변경 데이터 처리
    list.forEach(data -> { // 각 변경된 행
        String database = data.getDatabase();
        String table = data.getTable(); // member
        DataRow afterDataRow = data.getDataRow(); // 변경후 이미지
        if (data.getType() == ChangeType.UPDATE) {
            DataRow beforeDataRow = data.getDataRowBeforeUpdate(); // 변경전 이미지
            Long id = beforeDataRow.getLong("id"); // 변경전 이미지는 PK 칼럼만 포함
            String name = afterDataRow.getString("name"); // 변경후 이미지는 변경된 칼럼만 포함
            // ...
        }
    });
}
```

### 마리아DB 10.5: binlog_row_metadata = full

마리아DB 10.5는 binlog_row_metadata 설정 변수를 지원한다.
binlog_row_metadata 값이 FULL이면 (칼럼 이름을 포함한) 모든 메타데이터를 기록한다.
그래서 binlog_row_metadata가 FULL이면 ColumnNamesGetter가 필요 없다.

## Binlog 리더구현체

두 개의 binlog 리더를 지원:
* DefaultBinaryLogWrapper (기본)
* BinLogReaderBinaryLogWrapper (실험)

기본적으로 사용하는 binlog 리더는 DefaultBinaryLogWrapper이다.
DefaultBinaryLogWrapper는 mysql-binlog-connector-java(shyiko binlog)를 사용한다.

리더를 변경하고 싶으면 MariadbCdcConfig#setBinaryLogWrapperFactoryClass 메서드를 사용하면 된다: 

```
config.setBinaryLogWrapperFactoryClass(BinLogReaderBinaryLogWrapperFactory.class);
```

### BinLogReaderBinaryLogWrapper 제한

* 사용자/암호 인증만 지원
* SSL 미지원
* gtid 미지원