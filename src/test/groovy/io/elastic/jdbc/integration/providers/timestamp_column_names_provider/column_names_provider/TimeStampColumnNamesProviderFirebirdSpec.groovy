package io.elastic.jdbc.integration.providers.timestamp_column_names_provider.column_names_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.TimeStampColumnNamesProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class TimeStampColumnNamesProviderFirebirdSpec extends Specification {
    @Shared
    Connection connection
    @Shared
    JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
            .add("tableName", "STARS")
            .build()
    @Shared
    String sqlDropTable = "DROP TABLE STARS"
    @Shared
    String sqlCreateTable = "RECREATE TABLE STARS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION float, CREATEDAT timestamp)"

    def setup() {
        connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
        connection.createStatement().execute(sqlCreateTable);
    }

    def cleanupSpec() {
        connection.createStatement().execute(sqlDropTable);
        connection.close()
    }

    def "get select model, given table name"() {
        TimeStampColumnNamesProvider provider = new TimeStampColumnNamesProvider()
        JsonObject meta = provider.getSelectModel(config)
        print meta
        expect:
        meta.toString() == "{\"CREATEDAT\":\"CREATEDAT\"}"
    }

}
