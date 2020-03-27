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
    String sqlDropUsersTable = "EXECUTE BLOCK AS BEGIN\n" +
            "if (exists(select 1 from rdb\$relations where rdb\$relation_name = 'USERS')) then\n" +
            "execute statement 'DROP TABLE USERS;';\n" +
            "END"
    @Shared
    String sqlDropOrdersTable = "EXECUTE BLOCK AS BEGIN\n" +
            "if (exists(select 1 from rdb\$relations where rdb\$relation_name = 'ORDERS')) then\n" +
            "execute statement 'DROP TABLE ORDERS;';\n" +
            "END"
    @Shared
    String sqlDropProductsTable = "EXECUTE BLOCK AS BEGIN\n" +
            "if (exists(select 1 from rdb\$relations where rdb\$relation_name = 'PRODUCTS')) then\n" +
            "execute statement 'DROP TABLE PRODUCTS;';\n" +
            "END"
    @Shared
    String sqlCreateUsersTable = "EXECUTE BLOCK AS BEGIN\n" +
            "if (not exists(select 1 from rdb\$relations where rdb\$relation_name = 'USERS')) then\n" +
            "execute statement 'CREATE TABLE USERS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION int);';\n" +
            "END"
    @Shared
    String sqlCreateOrdersTable = "EXECUTE BLOCK AS BEGIN\n" +
            "if (not exists(select 1 from rdb\$relations where rdb\$relation_name = 'ORDERS')) then\n" +
            "execute statement 'CREATE TABLE ORDERS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION int);';\n" +
            "END"
    @Shared
    String sqlCreateProductsTable = "EXECUTE BLOCK AS BEGIN\n" +
            "if (not exists(select 1 from rdb\$relations where rdb\$relation_name = 'PRODUCTS')) then\n" +
            "execute statement 'CREATE TABLE PRODUCTS (ID int, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION int);';\n" +
            "END"

    def setupSpec() {
        connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
        connection.createStatement().execute(sqlDropUsersTable);
        connection.createStatement().execute(sqlDropProductsTable);
        connection.createStatement().execute(sqlDropOrdersTable);
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
