package io.elastic.jdbc.integration.providers.primary_column_names_provider

import io.elastic.jdbc.providers.PrimaryColumnNamesProvider
import spock.lang.*

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class PrimaryColumnNamesProviderMySQLSpec extends Specification {

  @Shared
  def connectionString = System.getenv("CONN_URI_MYSQL")
  @Shared
  def user = System.getenv("CONN_USER_MYSQL")
  @Shared
  def password = System.getenv("CONN_PASSWORD_MYSQL")
  @Shared
  def databaseName = System.getenv("CONN_DBNAME_MYSQL")
  @Shared
  def host = System.getenv("CONN_HOST_MYSQL")

  @Shared
  Connection connection

  def setup() {
    connection = DriverManager.getConnection(connectionString, user, password);
    String sql = "DROP TABLE IF EXISTS stars"
    connection.createStatement().execute(sql)
    sql = "CREATE TABLE stars (ID int, name varchar(255) NOT NULL, radius int, destination float, createdat DATETIME, PRIMARY KEY (ID))"
    connection.createStatement().execute(sql);
  }

  def cleanupSpec() {
    String sql = "DROP TABLE IF EXISTS stars"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "get metadata model, given table name"() {

    JsonObjectBuilder config = Json.createObjectBuilder()
    config.add("user", user)
        .add("password", password)
        .add("dbEngine", "mysql")
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
