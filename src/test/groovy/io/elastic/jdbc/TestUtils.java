package io.elastic.jdbc;

import io.github.cdimascio.dotenv.Dotenv;
import javax.json.Json;
import javax.json.JsonObjectBuilder;

public class TestUtils {

  static Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

  public static JsonObjectBuilder getMssqlConfigurastionBuilder() {
    return Json.createObjectBuilder()
        .add("dbEngine", "mssql")
        .add("host", dotenv.get("CONN_HOST_MSSQL"))
        .add("port", dotenv.get("CONN_PORT_MSSQL"))
        .add("databaseName", dotenv.get("CONN_DBNAME_MSSQL"))
        .add("user", dotenv.get("CONN_USER_MSSQL"))
        .add("password", dotenv.get("CONN_PASSWORD_MSSQL"));
  }

  public static JsonObjectBuilder getMysqlConfigurastionBuilder() {
    return Json.createObjectBuilder()
        .add("dbEngine", "mysql")
        .add("host", dotenv.get("CONN_HOST_MYSQL"))
        .add("port", dotenv.get("CONN_PORT_MYSQL"))
        .add("databaseName", dotenv.get("CONN_DBNAME_MYSQL"))
        .add("user", dotenv.get("CONN_USER_MYSQL"))
        .add("password", dotenv.get("CONN_PASSWORD_MYSQL"));
  }

  public static JsonObjectBuilder getPostgresqlConfigurastionBuilder() {
    return Json.createObjectBuilder()
        .add("dbEngine", "postgresql")
        .add("host", dotenv.get("CONN_HOST_POSTGRESQL"))
        .add("port", dotenv.get("CONN_PORT_POSTGRESQL"))
        .add("databaseName", dotenv.get("CONN_DBNAME_POSTGRESQL"))
        .add("user", dotenv.get("CONN_USER_POSTGRESQL"))
        .add("password", dotenv.get("CONN_PASSWORD_POSTGRESQL"));
  }

  public static JsonObjectBuilder getOracleConfigurastionBuilder() {
    return Json.createObjectBuilder()
        .add("dbEngine", "oracle")
        .add("host", dotenv.get("CONN_HOST_ORACLE"))
        .add("port", dotenv.get("CONN_PORT_ORACLE"))
        .add("databaseName", dotenv.get("CONN_DBNAME_ORACLE"))
        .add("user", dotenv.get("CONN_USER_ORACLE"))
        .add("password", dotenv.get("CONN_PASSWORD_ORACLE"));
  }
}
