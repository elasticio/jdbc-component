package io.elastic.jdbc.integration.providers.table_name_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.TableNameProvider
import spock.lang.Shared
import spock.lang.Specification

import jakarta.json.JsonObject
import jakarta.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

class TableNameProviderMySQLSpec extends Specification {

    @Shared
    Connection connection

    @Shared
    JsonObject config

    def setupSpec() {
        Class.forName("com.mysql.jdbc.Driver");
        JsonObject config = TestUtils.getMysqlConfigurationBuilder().build()
        connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))


        connection.createStatement().execute(" DROP TABLE IF EXISTS users");
        connection.createStatement().execute(" DROP TABLE IF EXISTS products");
        connection.createStatement().execute(" DROP TABLE IF EXISTS orders");
    }

    def cleanupSpec() {
        connection.createStatement().execute(" DROP TABLE IF EXISTS users");
        connection.createStatement().execute(" DROP TABLE IF EXISTS products");
        connection.createStatement().execute(" DROP TABLE IF EXISTS orders");

        connection.close();
    }

    def "create tables, successful"() {

        JsonObjectBuilder config = TestUtils.getMysqlConfigurationBuilder()
                .add("tableName", "stars")

        String sql1 = "CREATE TABLE users (id int, name varchar(255) NOT NULL, radius int, destination int)"
        String sql2 = "CREATE TABLE products (id int, name varchar(255) NOT NULL, radius int, destination int)"
        String sql3 = "CREATE TABLE orders (id int, name varchar(255) NOT NULL, radius int, destination int)"

        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        connection.createStatement().execute(sql3);


        TableNameProvider provider = new TableNameProvider();

        when:
        JsonObject model = provider.getSelectModel(config.build());

        then:
        print model
        model.getString("orders").equals("orders")
        model.getString("products").equals("products")
        model.getString("users").equals("users")
    }
}
