package io.elastic.jdbc;

import io.github.cdimascio.dotenv.Dotenv;
import javax.json.Json;
import javax.json.JsonObjectBuilder;

public class TestUtils {

  private static Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

  public static JsonObjectBuilder getMssqlConfigurationBuilder() {
    final String host = dotenv.get("CONN_HOST_MSSQL");
    final String port = dotenv.get("CONN_PORT_MSSQL");
    final String databaseName = dotenv.get("CONN_DBNAME_MSSQL");
    final String connectionString =
        "jdbc:sqlserver://" + host + ":" + port + ";databaseName=" + databaseName;
    return Json.createObjectBuilder()
        .add("dbEngine", "mssql")
        .add("host", host)
        .add("port", port)
        .add("databaseName", databaseName)
        .add("user", dotenv.get("CONN_USER_MSSQL"))
        .add("password", dotenv.get("CONN_PASSWORD_MSSQL"))
        .add("connectionString", connectionString);
  }

  public static JsonObjectBuilder getMysqlConfigurationBuilder() {
    final String host = dotenv.get("CONN_HOST_MYSQL");
    final String port = dotenv.get("CONN_PORT_MYSQL");
    final String databaseName = dotenv.get("CONN_DBNAME_MYSQL");
    final String connectionString =
        "jdbc:mysql://" + host + ":" + port + "/" + databaseName;
    return Json.createObjectBuilder()
        .add("dbEngine", "mysql")
        .add("host", host)
        .add("port", port)
        .add("databaseName", databaseName)
        .add("user", dotenv.get("CONN_USER_MYSQL"))
        .add("password", dotenv.get("CONN_PASSWORD_MYSQL"))
        .add("connectionString", connectionString);
  }

  public static JsonObjectBuilder getPostgresqlConfigurationBuilder() {
    final String host = dotenv.get("CONN_HOST_POSTGRESQL");
    final String port = dotenv.get("CONN_PORT_POSTGRESQL");
    final String databaseName = dotenv.get("CONN_DBNAME_POSTGRESQL");
    final String connectionString =
        "jdbc:postgresql://" + host + ":" + port + "/" + databaseName;
    return Json.createObjectBuilder()
        .add("dbEngine", "postgresql")
        .add("host", host)
        .add("port", port)
        .add("databaseName", databaseName)
        .add("user", dotenv.get("CONN_USER_POSTGRESQL"))
        .add("password", dotenv.get("CONN_PASSWORD_POSTGRESQL"))
        .add("connectionString", connectionString);
  }

  public static JsonObjectBuilder getOracleConfigurationBuilder() {
    final String host = dotenv.get("CONN_HOST_ORACLE");
    final String port = dotenv.get("CONN_PORT_ORACLE");
    final String databaseName = dotenv.get("CONN_DBNAME_ORACLE");
    final String connectionString =
        "jdbc:oracle:thin:@//" + host + ":" + port;
    return Json.createObjectBuilder()
        .add("dbEngine", "oracle")
        .add("host", host)
        .add("port", port)
        .add("databaseName", databaseName)
        .add("user", dotenv.get("CONN_USER_ORACLE"))
        .add("password", dotenv.get("CONN_PASSWORD_ORACLE"))
        .add("connectionString", connectionString);
  }
}
