package io.elastic.jdbc.integration.providers.procedure_fields_name_provider

import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.providers.ProcedureFieldsNameProvider
import io.elastic.jdbc.providers.SchemasProvider
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class ProcedureFieldsNameProviderMySQLSpec extends Specification {


    @Shared
    Connection connection

    @Shared
    SchemasProvider schemasProvider
    @Shared
    ProcedureFieldsNameProvider procedureProvider

    def setupSpec() {
        JsonObject config = getStarsConfig()
        connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
    }

    def setup() {
        prepareAction()
    }

    def prepareAction() {
        schemasProvider = new SchemasProvider()
        procedureProvider = new ProcedureFieldsNameProvider()
    }

    def runSchemasList(JsonObject config) {
        return schemasProvider.getSelectModel(config)
    }

    def runProcedureList(JsonObject config) {
        return procedureProvider.getProceduresList(config);
    }

    def runProcedureMetadata(JsonObject config) {
        return procedureProvider.getMetaModel(config);
    }

    def getStarsConfig() {
        JsonObject config = TestUtils.getMysqlConfigurationBuilder()
                .add("schemaName", "ELASTICIO")
                .add("procedureName", "GET_CUSTOMER_BY_ID_AND_NAME")
                .build()
        return config
    }

    def prepareStarsTable() {
        String sql = "DROP TABLE IF EXISTS CUSTOMERS"
        connection.createStatement().execute(sql)
        connection.createStatement().execute("create table CUSTOMERS\n" +
                "(\n" +
                "    PID      INT not null primary key,\n" +
                "    NAME     VARCHAR(128),\n" +
                "    CITY     VARCHAR(256),\n" +
                "    JOINDATE TIMESTAMP(6)\n" +
                ")");

        connection.createStatement().execute("DROP PROCEDURE IF EXISTS GET_CUSTOMER_BY_ID_AND_NAME;")
        connection.createStatement().execute("create\n" +
                "\t   definer = elasticio@`%` procedure GET_CUSTOMER_BY_ID_AND_NAME(\n" +
                "\t   IN p_cus_id INT,\n" +
                "\t   INOUT o_name VARCHAR(128),\n" +
                "\t   OUT o_city VARCHAR(128),\n" +
                "\t   OUT o_date TIMESTAMP(6))\n" +
                "BEGIN\n" +
                "  SELECT NAME , CITY, JOINDATE\n" +
                "  INTO o_name, o_city, o_date\n" +
                "  from  CUSTOMERS\n" +
                "  WHERE PID = p_cus_id\n" +
                "  AND NAME = o_name;\n" +
                "END;");

        connection.createStatement().execute("DROP PROCEDURE IF EXISTS GET_CUSTOMER_BY_ID;")
        connection.createStatement().execute("create\n" +
                "\t   definer = elasticio@`%` procedure GET_CUSTOMER_BY_ID(\n" +
                "\t   INOUT p_cus_id INT,\n" +
                "\t   OUT o_name VARCHAR(128),\n" +
                "\t   OUT o_city VARCHAR(128),\n" +
                "\t   OUT o_date TIMESTAMP(6))\n" +
                "BEGIN\n" +
                "  SELECT PID, NAME, CITY, JOINDATE\n" +
                "  INTO p_cus_id, o_name, o_city, o_date\n" +
                "  from  CUSTOMERS\n" +
                "  WHERE PID = p_cus_id;\n" +
                "END;");

        connection.createStatement().execute("INSERT INTO CUSTOMERS (PID,NAME,CITY,JOINDATE) VALUES (1,'Alice','Kyiv',STR_TO_DATE('2019-07-09 09:20:30', '%Y-%m-%d %H:%i:%s'))")
        connection.createStatement().execute("INSERT INTO CUSTOMERS (PID,NAME,CITY,JOINDATE) VALUES (2,'Bob','London',STR_TO_DATE('2019-07-09 09:20:30', '%Y-%m-%d %H:%i:%s'))")
    }

    def cleanupSpec() {
        connection.createStatement().execute("DROP TABLE IF EXISTS CUSTOMERS")
        connection.createStatement().execute("DROP PROCEDURE GET_CUSTOMER_BY_ID_AND_NAME")
        connection.close()
    }

    def "get schemas list"() {

        prepareStarsTable();

        when:
        def result = runSchemasList(getStarsConfig())
        def databaseName = getStarsConfig().getString("databaseName")
        then:
        result.getString(databaseName) == databaseName
        and:
        result.size() == 1
    }

    def "get procedure list"() {

        prepareStarsTable();

        def result = runProcedureList(getStarsConfig())
        expect:
        result.contains("GET_CUSTOMER_BY_ID")
        result.contains("GET_CUSTOMER_BY_ID_AND_NAME")
    }

    def "get procedure metadata"() {

        prepareStarsTable();

        when:
        def result = runProcedureMetadata(getStarsConfig())
        then:
        result.toString() == "{\"in\":{\"type\":\"object\",\"properties\":{\"p_cus_id\":{\"type\":\"number\",\"name\":\"p_cus_id\",\"required\":true},\"o_name\":{\"type\":\"string\",\"name\":\"o_name\",\"required\":true}}},\"out\":{\"type\":\"object\",\"properties\":{\"o_name\":{\"type\":\"string\",\"name\":\"o_name\",\"required\":true},\"o_city\":{\"type\":\"string\",\"name\":\"o_city\",\"required\":true},\"o_date\":{\"type\":\"string\",\"name\":\"o_date\",\"required\":true}}}}"
    }
}
