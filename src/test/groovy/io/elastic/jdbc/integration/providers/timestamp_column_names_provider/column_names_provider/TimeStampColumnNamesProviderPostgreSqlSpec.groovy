package io.elastic.jdbc.integration.providers.timestamp_column_names_provider.column_names_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.TimeStampColumnNamesProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class TimeStampColumnNamesProviderPostgreSqlSpec extends Specification {
    @Shared
    Connection connection
    @Shared
    JsonObject configuration
    @Shared
    String dbEngine = "postgresql"


    def setupSpec() {
        configuration = TestUtils.getPostgresqlConfigurationBuilder()
                .add("tableName", TestUtils.TEST_TABLE_NAME)
                .build()
        connection = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"));
        TestUtils.deleteTestTable(connection, dbEngine)
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
        meta.toString() == "{\"createdat\":\"createdat\"}"
    }

}
