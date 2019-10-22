package io.elastic.jdbc.integration.providers.timestamp_column_names_provider.column_names_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.TimeStampColumnNamesProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class TimeStampColumnNamesProviderOracleSpec extends Specification {
    @Shared
    Connection connection
    @Shared
    JsonObject configuration
    @Shared
    String dbEngine = "oracle"


    def setupSpec() {
        configuration = TestUtils.getOracleConfigurationBuilder()
            .add("tableName", "ELASTICIO." + TestUtils.TEST_TABLE_NAME.toUpperCase())
            .build()
        connection = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"))
        TestUtils.createTestTable(connection, dbEngine)
    }

    def cleanupSpec() {
        TestUtils.deleteTestTable(connection, dbEngine)
        connection.close()
    }

    def "get select model, given table name"() {
        TimeStampColumnNamesProvider provider = new TimeStampColumnNamesProvider()
        JsonObject meta = provider.getSelectModel(configuration)
        print meta
        expect:
        meta.toString() == "{\"CREATEDAT\":\"CREATEDAT\"}"
    }

}
