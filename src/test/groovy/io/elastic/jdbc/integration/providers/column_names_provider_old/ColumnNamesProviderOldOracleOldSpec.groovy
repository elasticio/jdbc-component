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
class ColumnNamesProviderOldOracleOldSpec extends Specification {
    @Shared def connectionString = ""
    @Shared def user = ""
    @Shared def password = ""
    @Shared Connection connection

    def setup() {
        connection = DriverManager.getConnection(connectionString, user, password);
        String sql = "BEGIN" +
                "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
                "EXCEPTION" +
                "   WHEN OTHERS THEN" +
                "      IF SQLCODE != -942 THEN" +
                "         RAISE;" +
                "      END IF;" +
                "END;"
        connection.createStatement().execute(sql);
        sql = "CREATE TABLE stars (ID number, name varchar(255) NOT NULL, radius number, destination float)"
        connection.createStatement().execute(sql);
        connection.close();
    }

    def "get metadata model, given table name" () {

        JsonObject config = new JsonObject()
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "oracle")
        config.addProperty("host", "")
        config.addProperty("databaseName", "ORCL")
        config.addProperty("tableName", "ELASTICIO.STARS")
        ColumnNamesProviderOld provider = new ColumnNamesProviderOld()
        JsonObject meta = SailorVersionsAdapter.javaxToGson(provider.getMetaModel(SailorVersionsAdapter.gsonToJavax(config)));
        print meta
        expect: meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"NAME\":{\"required\":true,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"NAME\":{\"required\":true,\"title\":\"NAME\",\"type\":\"string\"},\"RADIUS\":{\"required\":false,\"title\":\"RADIUS\",\"type\":\"number\"},\"DESTINATION\":{\"required\":false,\"title\":\"DESTINATION\",\"type\":\"number\"}}}}"
    }
}
