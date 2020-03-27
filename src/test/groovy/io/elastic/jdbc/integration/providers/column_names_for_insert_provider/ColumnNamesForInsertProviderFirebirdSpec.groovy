package io.elastic.jdbc.integration.providers.column_names_for_insert_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.ColumnNamesForInsertProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonReader
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesForInsertProviderFirebirdSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
          .add("tableName", "STARS")
          .build()
  @Shared
  String sqlDropTable = "EXECUTE BLOCK AS BEGIN\n" +
          "if (exists(select 1 from rdb\$relations where rdb\$relation_name = 'STARS')) then\n" +
          "execute statement 'DROP TABLE STARS;';\n" +
          "END"
  @Shared
  String sqlCreateTable = "EXECUTE BLOCK AS BEGIN\n" +
          "if (not exists(select 1 from rdb\$relations where rdb\$relation_name = 'STARS')) then\n" +
          "execute statement 'CREATE TABLE STARS (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, radius INT NOT NULL, destination FLOAT, createdat TIMESTAMP, diameter INT GENERATED ALWAYS AS (radius * 2));';\n" +
          "END"

  def setup() {
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
    connection.createStatement().execute(sqlDropTable)
    connection.createStatement().execute(sqlCreateTable)
  }

  def cleanupSpec() {
    connection.createStatement().execute(sqlDropTable)
    connection.close()
  }

  def "get metadata model, given table name"() {
    ColumnNamesForInsertProvider provider = new ColumnNamesForInsertProvider()
    JsonObject meta = provider.getMetaModel(config)
    InputStream fis = new FileInputStream("src/test/resources/GeneratedMetadata/columnNameFirebird.json");
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
