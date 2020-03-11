package io.elastic.jdbc.integration.providers.primary_column_names_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.PrimaryColumnNamesProvider
import spock.lang.*

import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

class PrimaryColumnNamesProviderMySQLSpec extends Specification {


  @Shared
  Connection connection

  def setup() {
    JsonObject config = TestUtils.getMysqlConfigurationBuilder().build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
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

    JsonObjectBuilder config = TestUtils.getMysqlConfigurationBuilder()
        .add("tableName", "stars")
    PrimaryColumnNamesProvider provider = new PrimaryColumnNamesProvider()
    JsonObject meta = provider.getMetaModel(config.build());
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}}}"
  }
}
