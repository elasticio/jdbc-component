package io.elastic.jdbc.integration.providers.column_names_provider_old

import io.elastic.jdbc.providers.ColumnNamesProviderOld
import io.elastic.jdbc.utils.SailorVersionsAdapter
import spock.lang.Ignore
import spock.lang.Specification
import com.google.gson.JsonObject

import java.sql.*

@Ignore
@Deprecated
class ColumnNamesProviderOldSpec extends Specification {

    def setup() {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:tests", "sa", "");
        String sql = "DROP TABLE IF EXISTS STARS";
        connection.createStatement().execute(sql);
        sql = "CREATE TABLE STARS (id int IDENTITY NOT NULL, isDead boolean, name varchar(255) NOT NULL, radius int, destination float)";
        connection.createStatement().execute(sql);
    }

    def cleanup() {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:tests", "sa", "");
        String sql = "DROP TABLE IF EXISTS STARS";
        connection.createStatement().execute(sql);
    }

    def "get metadata model, given table name" () {

        JsonObject config = new JsonObject()
        config.addProperty("dbEngine", "hsqldb")
        config.addProperty("user", "sa")
        config.addProperty("tableName", "STARS")
        config.addProperty("host", "localhost")
        config.addProperty("databaseName", "tests")
        ColumnNamesProviderOld provider = new ColumnNamesProviderOld()

        expect: provider.getMetaModel((SailorVersionsAdapter.gsonToJavax(config))).toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"ISDEAD\":{\"required\":false,\"title\":\"ISDEAD\",\"type\":\"boolean\"},\"NAME\":{\"required\":true,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"ISDEAD\":{\"required\":false,\"title\":\"ISDEAD\",\"type\":\"boolean\"},\"NAME\":{\"required\":true,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"}}}}"
    }

    def "should throw the exception when tableName is not set" () {
        JsonObject config = new JsonObject()
        config.addProperty("engine", "hsqldb")
        config.addProperty("user", " ")
        config.addProperty("password", " ")
        when:
        ColumnNamesProviderOld provider = new ColumnNamesProviderOld()
        provider.getColumns(SailorVersionsAdapter.gsonToJavax(config))
        then:
        def e = thrown(RuntimeException)
        e.message ==  "Table name is required"
    }
}
