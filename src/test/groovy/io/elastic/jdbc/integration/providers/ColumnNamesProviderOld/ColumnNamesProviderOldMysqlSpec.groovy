package io.elastic.jdbc.integration.providers.ColumnNamesProviderOld

import com.google.gson.JsonObject
import io.elastic.jdbc.ColumnNamesProviderOld
import io.elastic.jdbc.SailorVersionsAdapter
import spock.lang.Ignore
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Deprecated
@Ignore
class ColumnNamesProviderOldMysqlSpec extends Specification {

    def setup() {
        Connection connection = DriverManager.getConnection("", "", "");
        String sql = "DROP TABLE IF EXISTS stars";
        connection.createStatement().execute(sql);
        sql = "CREATE TABLE stars (id int NOT NULL AUTO_INCREMENT, isDead boolean, name varchar(255) NOT NULL, radius int, destination float, PRIMARY KEY (id))";
        connection.createStatement().execute(sql);
    }

    def cleanup() {
        Connection connection = DriverManager.getConnection("jdbc:mysql://sql4.freemysqlhosting.net/sql497581", "sql497581", "cSiRfebfuf");
        String sql = "DROP TABLE IF EXISTS stars";
        connection.createStatement().execute(sql);
    }

    def "get metadata model, given table name" () {

        JsonObject config = new JsonObject()
        config.addProperty("databaseName", "")
        config.addProperty("dbEngine", "mysql")
        config.addProperty("orderField", "id")
        config.addProperty("user", "")
        config.addProperty("password", "")
        config.addProperty("tableName", "stars")
        config.addProperty("host", "")
        ColumnNamesProviderOld provider = new ColumnNamesProviderOld()

        expect: provider.getMetaModel((SailorVersionsAdapter.gsonToJavax(config))).toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"id\":{\"required\":false,\"title\":\"id\",\"type\":\"number\"},\"isDead\":{\"required\":false,\"title\":\"isDead\",\"type\":\"boolean\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"id\":{\"required\":false,\"title\":\"id\",\"type\":\"number\"},\"isDead\":{\"required\":false,\"title\":\"isDead\",\"type\":\"boolean\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"}}}}"
    }
}
