package io.elastic.jdbc.integration.providers.column_names_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.ColumnNamesProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class ColumnNamesProviderFirebirdSpec extends Specification {
    @Shared
    Connection connection
    @Shared
    JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
            .add("tableName", "STARS")
            .build()
    @Shared
    String sqlDropTable = "DROP TABLE STARS"
    @Shared
    String sqlCreateTable = "RECREATE TABLE STARS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION float, CREATEDAT TIMESTAMP)"

    def setup() {
        connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
        connection.createStatement().execute(sqlCreateTable)
    }

    def cleanupSpec() {
        connection.createStatement().execute(sqlDropTable)
        connection.close()
    }

    def "get metadata model, given table name"() {
        ColumnNamesProvider provider = new ColumnNamesProvider()
        JsonObject meta = provider.getMetaModel(config)
        print meta
        expect:
        meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"NAME\":{\"required\":true,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"},\"CREATEDAT\":{\"required\":false,\"title\":\"CREATEDAT\",\"type\":\"string\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"NAME\":{\"required\":true,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"},\"CREATEDAT\":{\"required\":false,\"title\":\"CREATEDAT\",\"type\":\"string\"}}}}"
    }

}
