package io.elastic.jdbc.integration.providers.PrimaryColumnNamesProvider

import io.elastic.jdbc.PrimaryColumnNamesProvider
import spock.lang.*

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class PrimaryColumnNamesProviderOracleSpec extends Specification {

  @Shared
  def connectionString = System.getenv("CONN_URI_ORACLE")
  @Shared
  def user = System.getenv("CONN_USER_ORACLE")
  @Shared
  def password = System.getenv("CONN_PASSWORD_ORACLE")
  @Shared
  def databaseName = System.getenv("CONN_DBNAME_ORACLE")
  @Shared
  def host = System.getenv("CONN_HOST_ORACLE")

  @Shared
  Connection connection

  def setup() {
    connection = DriverManager.getConnection(connectionString, user, password);
    String sql = "BEGIN\n" +
        "   EXECUTE IMMEDIATE 'DROP TABLE stars';\n" +
        "EXCEPTION\n" +
        "   WHEN OTHERS THEN\n" +
        "      IF SQLCODE != -942 THEN\n" +
        "         RAISE;\n" +
        "      END IF;\n" +
        "END;"
    connection.createStatement().execute(sql)
    sql = "CREATE TABLE\n" +
        "    ELASTICIO.stars\n" +
        "    (\n" +
        "        ID INTEGER,\n" +
        "        name VARCHAR2(255) NOT NULL,\n" +
        "        raduis INTEGER,\n" +
        "        destination FLOAT,\n" +
        "        createdat DATE,\n" +
        "        PRIMARY KEY (ID)\n" +
        "    )"
    connection.createStatement().execute(sql);
  }

  def cleanupSpec() {
    String sql = "BEGIN\n" +
        "   EXECUTE IMMEDIATE 'DROP TABLE stars';\n" +
        "EXCEPTION\n" +
        "   WHEN OTHERS THEN\n" +
        "      IF SQLCODE != -942 THEN\n" +
        "         RAISE;\n" +
        "      END IF;\n" +
        "END;"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "get metadata model, given table name"() {

    JsonObjectBuilder config = Json.createObjectBuilder()
    config.add("user", user)
        .add("password", password)
        .add("dbEngine", "oracle")
        .add("host", host)
        .add("databaseName", databaseName)
        .add("tableName", "stars")
    PrimaryColumnNamesProvider provider = new PrimaryColumnNamesProvider()
    JsonObject meta = provider.getMetaModel(config.build());
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}}}"
  }
}
