package io.elastic.jdbc.integration.providers.column_names_for_insert_provider

import io.elastic.jdbc.providers.ColumnNamesForInsertProvider
import io.elastic.jdbc.TestUtils
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonReader
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesForInsertProviderPostgresqlSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config

  def setup() {
    config = TestUtils.getPostgresqlConfigurationBuilder()
        .add("tableName", "stars")
        .build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
    String sql = " DROP TABLE IF EXISTS stars;"
    connection.createStatement().execute(sql)
    sql = "CREATE TABLE stars (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, radius INT NOT NULL, destination FLOAT, createdat date);"
    connection.createStatement().execute(sql);
  }

  def cleanupSpec() {
    String sql = " DROP TABLE IF EXISTS stars;"
    connection.createStatement().execute(sql)
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
  }
}
