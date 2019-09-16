package io.elastic.jdbc.integration.providers.ColumnNamesProvider

import io.elastic.jdbc.ColumnNamesProvider
import io.elastic.jdbc.TestUtils
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesProviderMSSQLSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config

  def setup() {
    config = TestUtils.getMssqlConfigurationBuilder()
        .add("tableName", "stars")
        .build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
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


    ColumnNamesProvider provider = new ColumnNamesProvider()
    JsonObject meta = provider.getMetaModel(config)
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"},\"createdat\":{\"required\":false,\"title\":\"createdat\",\"type\":\"string\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"},\"createdat\":{\"required\":false,\"title\":\"createdat\",\"type\":\"string\"}}}}"
  }
}
