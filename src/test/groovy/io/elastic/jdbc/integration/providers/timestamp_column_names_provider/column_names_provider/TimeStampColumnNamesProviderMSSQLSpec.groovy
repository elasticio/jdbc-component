package io.elastic.jdbc.integration.providers.timestamp_column_names_provider.column_names_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.TimeStampColumnNamesProvider
import spock.lang.Shared
import spock.lang.Specification

import jakarta.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class TimeStampColumnNamesProviderMSSQLSpec extends Specification {

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

  def "get select model, given table name"() {


    TimeStampColumnNamesProvider provider = new TimeStampColumnNamesProvider()
    JsonObject meta = provider.getSelectModel(config)
    print meta
    expect:
    meta.toString() == "{\"createdat\":\"createdat\"}"
  }
}
