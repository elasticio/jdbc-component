package io.elastic.jdbc.integration.providers.primary_column_names_provider

import io.elastic.jdbc.providers.PrimaryColumnNamesProvider
import io.elastic.jdbc.TestUtils
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class PrimaryColumnNamesProviderMSSQLSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config

  def setup() {
    config = TestUtils.getMssqlConfigurationBuilder()
        .add("tableName", "stars")
        .build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
    String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql)
    sql = "CREATE TABLE stars (ID int, name varchar(255) NOT NULL, radius int, destination float, createdat DATETIME, CONSTRAINT id_pk PRIMARY KEY (ID))"
    connection.createStatement().execute(sql);
  }

  def cleanupSpec() {
    String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "get metadata model, given table name"() {

    PrimaryColumnNamesProvider provider = new PrimaryColumnNamesProvider()
    JsonObject meta = provider.getMetaModel(config);
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}}}"
  }
}
