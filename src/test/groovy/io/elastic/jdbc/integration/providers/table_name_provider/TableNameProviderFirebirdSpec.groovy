package io.elastic.jdbc.integration.providers.table_name_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.TableNameProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

class TableNameProviderFirebirdSpec extends Specification {

    @Shared
    Connection connection
    @Shared
    JsonObject config = TestUtils.getFirebirdConfigurationBuilder().build()
    @Shared
    String sqlDropUsersTable = "DROP TABLE USERS"
    @Shared
    String sqlDropOrdersTable = "DROP TABLE ORDERS"
    @Shared
    String sqlDropProductsTable = "DROP TABLE PRODUCTS"
    @Shared
    String sqlCreateUsersTable = "RECREATE TABLE USERS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION int)"
    @Shared
    String sqlCreateOrdersTable = "RECREATE TABLE ORDERS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION int)"
    @Shared
    String sqlCreateProductsTable = "RECREATE TABLE PRODUCTS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION int)"

    def setupSpec() {
        connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
        connection.createStatement().execute(sqlCreateUsersTable);
        connection.createStatement().execute(sqlCreateProductsTable);
        connection.createStatement().execute(sqlCreateOrdersTable);
    }

    def cleanupSpec() {
        connection.createStatement().execute(sqlDropUsersTable);
        connection.createStatement().execute(sqlDropProductsTable);
        connection.createStatement().execute(sqlDropOrdersTable);
        connection.close();
    }

    def "create tables, successful"() {
        JsonObjectBuilder config = TestUtils.getFirebirdConfigurationBuilder()
        TableNameProvider provider = new TableNameProvider();

        when:
        JsonObject model = provider.getSelectModel(config.build());

        then:
        print model
        model.getString("ORDERS").equals("ORDERS")
        model.getString("PRODUCTS").equals("PRODUCTS")
        model.getString("USERS").equals("USERS")
    }
}
