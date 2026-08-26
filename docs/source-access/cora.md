# CORA source-access discovery note

Status: validated connectivity from the Podsan R environment; no `redsan` CORA backend is implemented yet.

## Validated access path

CORA is exposed as an Oracle database. A live R connection was validated with:

```text
R
-> DBI
-> RJDBC
-> Oracle JDBC driver (`ojdbc17.jar`)
-> Oracle service `srv_CWPROD`
-> CORA
```

The smoke test

```sql
SELECT 1 AS test FROM dual
```

returned `TEST = 1`, and the JDBC connection disconnected cleanly afterwards.

## Configuration available in the D2IM keystore

The active `d2imr` keystore contains the following CORA entries:

```text
db.cora.url
db.cora.srv
db.cora.port
db.cora.usr
db.cora.pwd
```

`db.cora.url` is an Oracle Net descriptor containing:

- two Oracle VIP hosts: `oracle-bdd-p7-vip` and `oracle-bdd-p8-vip`
- TCP port `1521`
- `SERVICE_NAME=srv_CWPROD`
- load balancing enabled
- failover enabled

The full descriptor should be preferred over reconstructing a connection from only `db.cora.srv` and `db.cora.port`, because it preserves the configured second host and failover behaviour.

Do not print or persist `db.cora.pwd`; read it directly from the keystore at connection time.

## Runtime components present in Podsan

Validated in the environment:

```text
Java: OpenJDK 17
R package: DBI
R package: RJDBC
Oracle JDBC drivers:
  /opt/oracle/instantclient_23_26/ojdbc8.jar
  /opt/oracle/instantclient_23_26/ojdbc11.jar
  /opt/oracle/instantclient_23_26/ojdbc17.jar
```

`ojdbc17.jar` was used for the successful test.

Only the FreeTDS ODBC driver is registered in `odbc::odbcListDrivers()`, so the validated CORA path is JDBC rather than ODBC.

## Reproducible smoke test

```r
library(DBI)
library(RJDBC)

drv <- RJDBC::JDBC(
  "oracle.jdbc.OracleDriver",
  "/opt/oracle/instantclient_23_26/ojdbc17.jar"
)

jdbc_url <- paste0(
  "jdbc:oracle:thin:@",
  d2imr::d2im_keystore.get("db.cora.url")
)

con <- DBI::dbConnect(
  drv,
  jdbc_url,
  d2imr::d2im_keystore.get("db.cora.usr"),
  d2imr::d2im_keystore.get("db.cora.pwd")
)

DBI::dbGetQuery(con, "SELECT 1 AS test FROM dual")
DBI::dbDisconnect(con)
```

Expected smoke-test result:

```text
  TEST
1    1
```

## Existing `d2imr` Oracle helper

`d2imr` contains the internal function:

```r
d2imr:::d2im_dbc.oracle_execute_query
```

It should not be used as the basis for new CORA access without revision. The installed implementation is legacy because it:

- expects a `db.<env>.jdbc` keystore entry, while the current CORA configuration exposes `db.cora.url`;
- hard-codes `/appli/edsan_common/lib/j/ojdbc6.jar`;
- catches errors and returns `NULL`, which hides failure semantics;
- disconnects once in the main body and again in `finally`, so it can attempt a double disconnect.

A future `redsan` CORA transport should therefore use the current keystore entries directly and the Java-17-compatible Oracle driver rather than wrapping this helper unchanged.

## Current conclusion

CORA has moved from "configuration present in the keystore" to "source access experimentally validated". The next implementation step, if needed, is a small native R transport such as `query_cora()` with the same separation of concerns used for other `redsan` source-access backends: connection setup, parameterized read-only query execution, explicit errors, and source-specific retrieval only after table semantics have been validated.
