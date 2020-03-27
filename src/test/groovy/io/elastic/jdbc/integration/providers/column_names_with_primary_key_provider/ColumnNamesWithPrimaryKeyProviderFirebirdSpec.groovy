package io.elastic.jdbc.integration.providers.column_names_with_primary_key_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.ColumnNamesWithPrimaryKeyProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesWithPrimaryKeyProviderFirebirdSpec extends Specification {

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
          "execute statement 'CREATE TABLE STARS (ID int, ISDEAD smallint, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION float, CREATEDAT timestamp, PRIMARY KEY (ID));';\n" +
          "END"

  def setup() {
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
    connection.createStatement().execute(sqlDropTable)
    connection.createStatement().execute(sqlCreateTable)
  }

  def cleanupSpec() {
    connection.createStatement().execute(sqlDropTable)
    connection.close()
  }

  def "get metadata model, given table name"() {


    ColumnNamesWithPrimaryKeyProvider provider = new ColumnNamesWithPrimaryKeyProvider()
    JsonObject meta = provider.getMetaModel(config)
    print meta
    expect:
    meta.getJsonObject("in").toString() == "{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"},\"ISDEAD\":{\"required\":false,\"title\":\"ISDEAD\",\"type\":\"number\"},\"NAME\":{\"required\":false,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"},\"CREATEDAT\":{\"required\":false,\"title\":\"CREATEDAT\",\"type\":\"string\"}}}"
  }
}
