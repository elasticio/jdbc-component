package io.elastic.jdbc.integration.providers.column_names_for_insert_provider

import io.elastic.jdbc.providers.ColumnNamesForInsertProvider
import io.elastic.jdbc.TestUtils
import spock.lang.Shared
import spock.lang.Specification

import jakarta.json.Json
import jakarta.json.JsonObject
import jakarta.json.JsonReader
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesForInsertProviderOracleSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config

  def setup() {
    config = TestUtils.getOracleConfigurationBuilder()
        .add("tableName", "STARS")
        .build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
    String sql = "BEGIN" +
        "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
        "EXCEPTION" +
        "   WHEN OTHERS THEN" +
        "      IF SQLCODE != -942 THEN" +
        "         RAISE;" +
        "      END IF;" +
        "END;"
    connection.createStatement().execute(sql)
    sql = "CREATE TABLE stars (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, radius INT NOT NULL, destination FLOAT, createdat DATE)"
    connection.createStatement().execute(sql);
  }

  def cleanupSpec() {
    String sql = "BEGIN" +
        "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
        "EXCEPTION" +
        "   WHEN OTHERS THEN" +
        "      IF SQLCODE != -942 THEN" +
        "         RAISE;" +
        "      END IF;" +
        "END;"
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
    meta.getJsonObject("in").getJsonObject("properties").containsKey("ID")
  }
}
