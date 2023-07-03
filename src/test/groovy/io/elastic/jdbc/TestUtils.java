package io.elastic.jdbc;

import io.elastic.api.EventEmitter;
import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.SQLException;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;

public class TestUtils {

  public static final String TEST_TABLE_NAME = "stars";
  private static final String SQL_DELETE_TABLE =
      " DROP TABLE IF EXISTS " + TEST_TABLE_NAME;
  private static final String ORACLE_DELETE_TABLE = "BEGIN"
      + "   EXECUTE IMMEDIATE 'DROP TABLE "
      + TEST_TABLE_NAME + "';"
      + "EXCEPTION"
      + "   WHEN OTHERS THEN"
      + "      IF SQLCODE != -942 THEN"
      + "         RAISE;"
      + "      END IF;"
      + "END;";
  private static final String POSTGRESQL_CREATE_TABLE = "CREATE TABLE "
      + TEST_TABLE_NAME
      + " (id SERIAL PRIMARY KEY, "
      + "name VARCHAR(255) NOT NULL, "
      + "radius INT NOT NULL, "
      + "destination FLOAT, "
      + "visible boolean, "
      + "createdat timestamp)";
  private static final String MSSQL_CREATE_TABLE = "CREATE TABLE "
      + TEST_TABLE_NAME
      + " (id decimal(15,0) NOT NULL IDENTITY PRIMARY KEY NONCLUSTERED, "
      + "name varchar(255) NOT NULL, "
      + "radius int NOT NULL, "
      + "destination float, "
      + "visible bit, "
      + "createdat DATETIME, "
      + "diameter AS (radius*2))";
  private static final String ORACLE_CREATE_TABLE = "CREATE TABLE "
      + TEST_TABLE_NAME
      + " (id NUMBER PRIMARY KEY, "
      + "name VARCHAR(255) NOT NULL, "
      + "radius number NOT NULL, "
      + "destination FLOAT, "
      + "visible number(1), "
      + "createdat timestamp)";
  private static final String MYSQL_CREATE_TABLE = "CREATE TABLE "
      + TEST_TABLE_NAME
      + " (id INT AUTO_INCREMENT PRIMARY KEY, "
      + "name VARCHAR(255) NOT NULL, "
      + "radius INT NOT NULL, "
      + "destination FLOAT, "
      + "visible bit, "
      + "createdat DATETIME, "
      + "diameter INT GENERATED ALWAYS AS (radius * 2));";
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

  public static EventEmitter getFakeEventEmitter(EventEmitter.Callback fakeCallback) {
    return new EventEmitter.Builder()
        .onData(fakeCallback)
        .onSnapshot(fakeCallback)
        .onError(fakeCallback)
        .onRebound(fakeCallback)
        .onHttpReplyCallback(fakeCallback)
        .build();
  }

  public static void createTestTable(Connection connection, String dbEngine)
      throws SQLException {
    switch (dbEngine.toLowerCase()) {
      case "mysql":
        connection.createStatement().execute(MYSQL_CREATE_TABLE);
        break;
      case "mssql":
        connection.createStatement().execute(MSSQL_CREATE_TABLE);
        break;
      case "oracle":
        connection.createStatement().execute(ORACLE_CREATE_TABLE);
        break;
      case "postgresql":
        connection.createStatement().execute(POSTGRESQL_CREATE_TABLE);
        break;
      default:
        throw new RuntimeException("Unsupported dbEngine" + dbEngine);
    }
  }

  public static void deleteTestTable(Connection connection, String dbEngine)
      throws SQLException {
    if (dbEngine.toLowerCase().equals("oracle")){
      connection.createStatement().execute(ORACLE_DELETE_TABLE);
    } else {
      connection.createStatement().execute(SQL_DELETE_TABLE);
    }
  }
}
