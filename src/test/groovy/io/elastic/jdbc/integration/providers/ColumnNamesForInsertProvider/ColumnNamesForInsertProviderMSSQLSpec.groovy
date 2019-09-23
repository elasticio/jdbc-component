package io.elastic.jdbc.integration.providers.ColumnNamesForInsertProvider

import io.elastic.jdbc.providers.ColumnNamesForInsertProvider
import io.elastic.jdbc.TestUtils
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonReader
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesForInsertProviderMSSQLSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config
  @Shared
  String tableName = "stars"
  @Shared
  String sqlCreateTable = "CREATE TABLE " +
      tableName +
      " (id decimal(15,0) NOT NULL IDENTITY PRIMARY KEY NONCLUSTERED, " +
      "name varchar(255) NOT NULL, " +
      "radius int NOT NULL, " +
      "destination float, " +
      "createdat DATETIME, " +
      "diameter AS (radius*2))"
  @Shared
  String sqlDeleteTable = " DROP TABLE IF EXISTS " + tableName

  def setup() {
    config = TestUtils.getMssqlConfigurationBuilder()
        .add("tableName", tableName)
        .build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
    createTable()
  }

  def createTable() {
    connection.createStatement().execute(sqlCreateTable);
  }

  def deleteTable() {
    connection.createStatement().execute(sqlDeleteTable);
  }

  def cleanupSpec() {
    deleteTable()
    connection.close()
  }

  def "get metadata model, given table name"() {
    ColumnNamesForInsertProvider provider = new ColumnNamesForInsertProvider()
    JsonObject meta = provider.getMetaModel(config)
    InputStream fis = new FileInputStream("src/test/resources/GeneratedMetadata/columnName.json");
    JsonReader reader = Json.createReader(fis);
    JsonObject expectedMetadata = reader.readObject();
    reader.close();
    print meta
    expect:
    meta.containsKey("in")
    meta.containsKey("out")
    meta.getJsonObject("in") == expectedMetadata.getJsonObject("in")
    meta.getJsonObject("out") == expectedMetadata.getJsonObject("out")
  }
}
