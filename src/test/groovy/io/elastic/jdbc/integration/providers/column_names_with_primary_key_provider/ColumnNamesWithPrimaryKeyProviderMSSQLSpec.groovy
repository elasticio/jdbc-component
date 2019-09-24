package io.elastic.jdbc.integration.providers.column_names_with_primary_key_provider


import io.elastic.jdbc.providers.ColumnNamesWithPrimaryKeyProvider
import io.elastic.jdbc.TestUtils
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesWithPrimaryKeyProviderMSSQLSpec extends Specification {

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
    sql = "CREATE TABLE stars (ID decimal(15,0) NOT NULL IDENTITY PRIMARY KEY NONCLUSTERED, name varchar(255) NOT NULL, radius int, destination float, createdat DATETIME)"
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
    meta.getJsonObject("in").toString() == "{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":false,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"},\"createdat\":{\"required\":false,\"title\":\"createdat\",\"type\":\"string\"}}}"
  }
}
