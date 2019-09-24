package io.elastic.jdbc.integration.providers.column_names_provider_old

import com.google.gson.JsonObject
import io.elastic.jdbc.providers.ColumnNamesProviderOld
import io.elastic.jdbc.utils.SailorVersionsAdapter
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Deprecated
@Ignore
class ColumnNamesProviderOldMSSQLOldSpecOld extends Specification {
    @Shared def connectionString = ""
    @Shared def user = ""
    @Shared def password = ""
    @Shared Connection connection

    def setup() {
        connection = DriverManager.getConnection(connectionString, user, password);
        String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
                "  DROP TABLE stars;"
        connection.createStatement().execute(sql)
        sql = "CREATE TABLE stars (ID int, name varchar(255) NOT NULL, radius int, destination float)"
        connection.createStatement().execute(sql);
    }
    def cleanupSpec() {
        String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
                "  DROP TABLE stars;"
        connection.createStatement().execute(sql)
        connection.close()
    }
    def "get metadata model, given table name" () {

        JsonObject config = new JsonObject()
        config.addProperty("tableName", "stars")
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "mssql")
        config.addProperty("host", "")
        config.addProperty("databaseName", "")
        ColumnNamesProviderOld provider = new ColumnNamesProviderOld()
        JsonObject meta = provider.getMetaModel((SailorVersionsAdapter.gsonToJavax(config)));
        print meta
        expect: meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"}}}}"
    }
}
