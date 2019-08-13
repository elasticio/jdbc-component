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
class TableNameProviderOldOracleOldSpec extends Specification {

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
        config.addProperty("dbEngine", "oracle")
        config.addProperty("host", "")
        config.addProperty("databaseName", "ORCL")
    }

    def cleanupSpec() {

        connection.createStatement().execute("DROP TABLE elasticio.users");
        connection.createStatement().execute("DROP TABLE elasticio.products");
        connection.createStatement().execute("DROP TABLE elasticio.orders");

        connection.close();
    }

    def "get selectbox values, successful"() {


        String sql1 = "CREATE TABLE elasticio.users (id int, name varchar(255) NOT NULL, radius int, destination int)";
        String sql2 = "CREATE TABLE elasticio.products (id int, name varchar(255) NOT NULL, radius int, destination int)";
        String sql3 = "CREATE TABLE elasticio.orders (id int, name varchar(255) NOT NULL, radius int, destination int)";

        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        connection.createStatement().execute(sql3);


        TableNameProviderOld provider = new TableNameProviderOld();

        when:
        JsonObject model = SailorVersionsAdapter.javaxToGson(provider.getSelectModel(SailorVersionsAdapter.gsonToJavax(config)));

        then:

        model.get('ELASTICIO.ORDERS').getAsString() == 'ELASTICIO.ORDERS'
        model.get('ELASTICIO.PRODUCTS').getAsString() == 'ELASTICIO.PRODUCTS'
        model.get('ELASTICIO.USERS').getAsString() == 'ELASTICIO.USERS'
    }
}
