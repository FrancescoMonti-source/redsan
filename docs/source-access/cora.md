# CORA source access

Use `cora_query()` for one-off read-only queries and `db_connect("cora")`
for a connection shared across queries. Use `cora_observation_catalog()`
and `cora_get_observations()` for [CORA observation documents](cora-observations.md).

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

## Check a connection

```r
redsan::cora_query("SELECT 1 AS TEST FROM DUAL")
redsan::edsan_check_connections(oracle_query = TRUE)
```

The query should return `TEST = 1`. The diagnostic also checks EDSaN CT
and reports the DNS, TCP, and JDBC results for CORA.

Set `REDSAN_OJDBC_JAR` if the driver is installed in a different location,
or pass `ojdbc_jar` to `cora_query()` or `db_connect("cora")`.
Close a connection opened by `db_connect()` with `DBI::dbDisconnect()`.
