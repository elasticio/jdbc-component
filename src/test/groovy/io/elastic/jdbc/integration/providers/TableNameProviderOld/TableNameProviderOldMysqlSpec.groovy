package io.elastic.jdbc.integration.providers.TableNameProviderOld

import com.google.gson.JsonObject
import io.elastic.jdbc.Utils.SailorVersionsAdapter
import io.elastic.jdbc.providers.TableNameProviderOld
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Deprecated
@Ignore
class TableNameProviderOldMysqlSpec extends Specification {

    @Shared def connectionString = ""
    @Shared def user = ""
    @Shared def password = ""

    @Shared Connection connection

    @Shared JsonObject config

    def setupSpec() {
        connection = DriverManager.getConnection(connectionString, user, password)
        config = new JsonObject()
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "mysql")
        config.addProperty("host", "")
        config.addProperty("databaseName", "")
    }

    def cleanupSpec() {
        connection.createStatement().execute("DROP TABLE IF EXISTS users");
        connection.createStatement().execute("DROP TABLE IF EXISTS products");
        connection.createStatement().execute("DROP TABLE IF EXISTS orders");
        connection.close();
    }

    def "get selectbox values, successful"() {


        String sql1 = "CREATE TABLE users (id int, name varchar(255) NOT NULL, radius int, destination int)";
        String sql2 = "CREATE TABLE products (id int, name varchar(255) NOT NULL, radius int, destination int)";
        String sql3 = "CREATE TABLE orders (id int, name varchar(255) NOT NULL, radius int, destination int)";

        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        connection.createStatement().execute(sql3);


        TableNameProviderOld provider = new TableNameProviderOld();

        JsonObject model = SailorVersionsAdapter.javaxToGson(provider.getSelectModel(SailorVersionsAdapter.gsonToJavax(config)));

        expect: model.toString().contains('"orders":"orders","products":"products"')
    }
}
