package io.elastic.jdbc.integration.providers.ColumnNamesWithPrimaryKeyProvider


import io.elastic.jdbc.ColumnNamesWithPrimaryKeyProvider
import io.elastic.jdbc.TestUtils
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesWithPrimaryKeyProviderMySQLSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config

  def setup() {
    config = TestUtils.getMysqlConfigurationBuilder()
        .add("tableName", "stars")
        .build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
    String sql = " DROP TABLE IF EXISTS stars;"
    connection.createStatement().execute(sql)
    sql = "CREATE TABLE stars (id int AUTO_INCREMENT, isDead boolean, name varchar(255) NOT NULL, radius int, destination float, createdat timestamp, PRIMARY KEY (id))"
    connection.createStatement().execute(sql);
  }

  def cleanupSpec() {
    String sql = " DROP TABLE IF EXISTS stars;"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "get metadata model, given table name"() {


    ColumnNamesWithPrimaryKeyProvider provider = new ColumnNamesWithPrimaryKeyProvider()
    JsonObject meta = provider.getMetaModel(config)
    print meta
    expect:
    meta.getJsonObject("in").toString() == "{\"type\":\"object\",\"properties\":{\"id\":{\"required\":true,\"title\":\"id\",\"type\":\"number\"},\"isDead\":{\"required\":false,\"title\":\"isDead\",\"type\":\"boolean\"},\"name\":{\"required\":false,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"},\"createdat\":{\"required\":false,\"title\":\"createdat\",\"type\":\"string\"}}}"
  }
}
