package io.elastic.jdbc.integration.providers.primary_column_names_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.PrimaryColumnNamesProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

class PrimaryColumnNamesProviderFirebirdSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config = TestUtils.getFirebirdConfigurationBuilder().build()
  @Shared
  String sqlDropTable = "DROP TABLE STARS"
  @Shared
  String sqlCreateTable = "RECREATE TABLE STARS (ID int NOT NULL, name varchar(255) NOT NULL, RADIUS int, DESTINATION float, CREATEDAT TIMESTAMP, PRIMARY KEY (ID))"

  def setup() {
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
    connection.createStatement().execute(sqlCreateTable)
  }

  def cleanupSpec() {
    connection.createStatement().execute(sqlDropTable)
    connection.close()
  }

  def "get metadata model, given table name"() {

    JsonObjectBuilder config = TestUtils.getFirebirdConfigurationBuilder()
        .add("tableName", "STARS")
    PrimaryColumnNamesProvider provider = new PrimaryColumnNamesProvider()
    JsonObject meta = provider.getMetaModel(config.build());
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}}}"
  }
}
