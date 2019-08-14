package io.elastic.jdbc.integration.providers.ColumnNamesProvider

import io.elastic.jdbc.ColumnNamesProvider
import spock.lang.*

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class ColumnNamesProviderMSSQLSpec extends Specification {

  @Shared
  def connectionString = System.getenv("CONN_URI_MSSQL")
  @Shared
  def user = System.getenv("CONN_USER_MSSQL")
  @Shared
  def password = System.getenv("CONN_PASSWORD_MSSQL")
  @Shared
  def databaseName = System.getenv("CONN_DBNAME_MSSQL")
  @Shared
  def host = System.getenv("CONN_HOST_MSSQL")

  @Shared
  Connection connection

  def setup() {
    connection = DriverManager.getConnection(connectionString, user, password);
    String sql = " DROP TABLE IF EXISTS stars;"
    connection.createStatement().execute(sql)
    sql = "CREATE TABLE stars (ID int, name varchar(255) NOT NULL, radius int, destination float, createdat DATETIME)"
    connection.createStatement().execute(sql);
  }

  def cleanupSpec() {
    String sql = " DROP TABLE IF EXISTS stars;"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "get metadata model, given table name"() {

    JsonObjectBuilder config = Json.createObjectBuilder()
    config.add("user", user)
        .add("password", password)
        .add("dbEngine", "mssql")
        .add("host", host)
        .add("databaseName", databaseName)
        .add("tableName", "stars")
    ColumnNamesProvider provider = new ColumnNamesProvider()
    JsonObject meta = provider.getMetaModel(config.build())
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"},\"createdat\":{\"required\":false,\"title\":\"createdat\",\"type\":\"string\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"},\"createdat\":{\"required\":false,\"title\":\"createdat\",\"type\":\"string\"}}}}"
  }
}
