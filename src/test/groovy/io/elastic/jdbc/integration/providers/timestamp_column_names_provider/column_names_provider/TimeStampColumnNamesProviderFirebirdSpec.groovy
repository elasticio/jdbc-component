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
    String sqlDropTable = "EXECUTE BLOCK AS BEGIN\n" +
            "if (exists(select 1 from rdb\$relations where rdb\$relation_name = 'STARS')) then\n" +
            "execute statement 'DROP TABLE STARS;';\n" +
            "END"
    @Shared
    String sqlCreateTable = "EXECUTE BLOCK AS BEGIN\n" +
            "if (not exists(select 1 from rdb\$relations where rdb\$relation_name = 'STARS')) then\n" +
            "execute statement 'CREATE TABLE STARS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION float, CREATEDAT timestamp);';\n" +
            "END"

    def setup() {
        connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
        connection.createStatement().execute(sqlDropTable);
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
