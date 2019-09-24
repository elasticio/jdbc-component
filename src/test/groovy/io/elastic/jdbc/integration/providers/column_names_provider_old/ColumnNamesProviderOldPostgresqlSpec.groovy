package io.elastic.jdbc.integration.providers.column_names_provider_old

import com.google.gson.JsonObject
import io.elastic.jdbc.providers.ColumnNamesProviderOld
import io.elastic.jdbc.utils.SailorVersionsAdapter
import spock.lang.Ignore
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Deprecated
@Ignore
class ColumnNamesProviderOldPostgresqlSpec extends Specification {

    def setup() {
        Connection connection = DriverManager.getConnection("", "", "");
        String sql = "DROP TABLE IF EXISTS stars";
        connection.createStatement().execute(sql);
        sql = "CREATE TABLE stars (id SERIAL NOT NULL, isDead boolean, name varchar(255) NOT NULL, radius int, destination float)";
        connection.createStatement().execute(sql);
        connection.close();
    }

    def cleanup() {
        Connection connection = DriverManager.getConnection("", "", "");
        String sql = "DROP TABLE IF EXISTS stars";
        connection.createStatement().execute(sql);
        connection.close();
    }

    def "get metadata model, given table name" () {

        JsonObject config = new JsonObject()
        config.addProperty("databaseName", "")
        config.addProperty("dbEngine", "postgresql")
        config.addProperty("user", "")
        config.addProperty("password", "")
        config.addProperty("tableName", "stars")
        config.addProperty("host", "")
        ColumnNamesProviderOld provider = new ColumnNamesProviderOld()

        expect: provider.getMetaModel((SailorVersionsAdapter.gsonToJavax(config))).toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"id\":{\"required\":false,\"title\":\"id\",\"type\":\"number\"},\"isdead\":{\"required\":false,\"title\":\"isdead\",\"type\":\"boolean\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"id\":{\"required\":false,\"title\":\"id\",\"type\":\"number\"},\"isdead\":{\"required\":false,\"title\":\"isdead\",\"type\":\"boolean\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"}}}}"
    }
}
