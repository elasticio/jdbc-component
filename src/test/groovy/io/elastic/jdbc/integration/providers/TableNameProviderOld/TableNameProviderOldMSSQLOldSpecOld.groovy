package io.elastic.jdbc.integration.providers.TableNameProviderOld
import com.google.gson.JsonObject
import io.elastic.jdbc.SailorVersionsAdapter
import io.elastic.jdbc.TableNameProviderOld
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Deprecated
@Ignore
class TableNameProviderOldMSSQLOldSpecOld extends Specification {

    @Shared def connectionString = ""
    @Shared def user = ""
    @Shared def password = ""

    @Shared Connection connection

    @Shared JsonObject config

    def setupSpec() {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(connectionString, user, password)
        config = new JsonObject()
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "mssql")
        config.addProperty("host", "")
        config.addProperty("databaseName", "")
    }

    def cleanupSpec() {
        connection.createStatement().execute("DROP TABLE users");
        connection.createStatement().execute("DROP TABLE products");
        connection.createStatement().execute("DROP TABLE orders");

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

        when:
        JsonObject model = provider.getSelectModel(SailorVersionsAdapter.gsonToJavax(config));

        then:
        print model
        model.toString().contains('"dbo.orders":"dbo.orders","dbo.products":"dbo.products"')
    }
}
