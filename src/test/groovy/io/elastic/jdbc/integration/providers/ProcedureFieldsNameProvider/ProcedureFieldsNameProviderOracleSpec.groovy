package io.elastic.jdbc.integration.providers.ProcedureFieldsNameProvider

import io.elastic.jdbc.providers.ProcedureFieldsNameProvider
import io.elastic.jdbc.providers.SchemasProvider
import org.junit.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class ProcedureFieldsNameProviderOracleSpec extends Specification {
    @Shared
    def user = System.getenv("CONN_USER_ORACLE")
    @Shared
    def password = System.getenv("CONN_PASSWORD_ORACLE")
    @Shared
    def databaseName = System.getenv("CONN_DBNAME_ORACLE")
    @Shared
    def host = System.getenv("CONN_HOST_ORACLE")
    @Shared
    def port = System.getenv("CONN_PORT_ORACLE")
    @Shared
    def connectionString = "jdbc:oracle:thin:@//" + host + ":" + port + "/XE"

    @Shared
    Connection connection

    @Shared
    SchemasProvider schemasProvider
    @Shared
    ProcedureFieldsNameProvider procedureProvider

    def setupSpec() {
        connection = DriverManager.getConnection(connectionString, user, password)
    }

    def setup() {
        prepareAction()
    }

    def prepareAction() {
        schemasProvider = new SchemasProvider()
        procedureProvider = new ProcedureFieldsNameProvider()
    }

    def runSchemasList(JsonObject config) {
        return schemasProvider.getSchemasList(config)
    }

    def runProcedureList(JsonObject config) {
        return procedureProvider.getProceduresList(config);
    }

    def runProcedureMetadata(JsonObject config) {
        return procedureProvider.getMetaModel(config);
    }

    def getStarsConfig() {
        JsonObject config = Json.createObjectBuilder()
                .add("schemaName", "ELASTICIO")
                .add("procedureName", "GET_CUSTOMER_BY_ID_AND_NAME")
                .add("user", user)
                .add("password", password)
                .add("dbEngine", "oracle")
                .add("host", host)
                .add("port", port)
                .add("databaseName", databaseName)
                .build();
        return config;
    }

    def prepareStarsTable() {
        String sql = "BEGIN" +
                "   EXECUTE IMMEDIATE 'DROP TABLE CUSTOMERS';" +
                "EXCEPTION" +
                "   WHEN OTHERS THEN" +
                "      IF SQLCODE != -942 THEN" +
                "         RAISE;" +
                "      END IF;" +
                "END;"
        connection.createStatement().execute(sql);
        connection.createStatement().execute("create table CUSTOMERS\n" +
                "(\n" +
                "    PID      NUMBER not null\n" +
                "        constraint CUSTOMERS_PK\n" +
                "            primary key,\n" +
                "    NAME     VARCHAR2(128),\n" +
                "    CITY     VARCHAR2(256),\n" +
                "    JOINDATE TIMESTAMP(6)\n" +
                ")");

        connection.createStatement().execute("create or replace PROCEDURE \"GET_CUSTOMER_BY_ID_AND_NAME\"(\n" +
                "\t   p_cus_id IN CUSTOMERS.PID%TYPE,\n" +
                "\t   o_name IN OUT CUSTOMERS.NAME%TYPE,\n" +
                "\t   o_city OUT  CUSTOMERS.CITY%TYPE,\n" +
                "\t   o_date OUT CUSTOMERS.JOINDATE%TYPE)\n" +
                "IS\n" +
                "BEGIN\n" +
                "  SELECT NAME , CITY, JOINDATE\n" +
                "  INTO o_name, o_city, o_date\n" +
                "  from  CUSTOMERS\n" +
                "  WHERE PID = p_cus_id\n" +
                "  AND NAME = o_name;\n" +
                "END;");

        connection.createStatement().execute("create or replace  PROCEDURE \"GET_CUSTOMER_BY_ID\"(\n" +
                "\t   p_cus_id IN OUT CUSTOMERS.PID%TYPE,\n" +
                "\t   o_name OUT CUSTOMERS.NAME%TYPE,\n" +
                "\t   o_city OUT  CUSTOMERS.CITY%TYPE,\n" +
                "\t   o_date OUT CUSTOMERS.JOINDATE%TYPE)\n" +
                "IS\n" +
                "BEGIN\n" +
                "  SELECT PID, NAME, CITY, JOINDATE\n" +
                "  INTO p_cus_id, o_name, o_city, o_date\n" +
                "  from  CUSTOMERS\n" +
                "  WHERE PID = p_cus_id;\n" +
                "END;");

        connection.createStatement().execute("INSERT INTO CUSTOMERS (PID,NAME,CITY,JOINDATE) VALUES (1,'Alice','Kyiv',TO_TIMESTAMP('2019-07-09 09:20:30.307000', 'YYYY-MM-DD HH24:MI:SS.FF6'))")
        connection.createStatement().execute("INSERT INTO CUSTOMERS (PID,NAME,CITY,JOINDATE) VALUES (2,'Bob','London',TO_TIMESTAMP('2019-07-09 09:20:30.307000', 'YYYY-MM-DD HH24:MI:SS.FF6'))")
    }

    def cleanupSpec() {
        String sql = "BEGIN" +
                "   EXECUTE IMMEDIATE 'DROP TABLE CUSTOMERS';" +
                "EXCEPTION" +
                "   WHEN OTHERS THEN" +
                "      IF SQLCODE != -942 THEN" +
                "         RAISE;" +
                "      END IF;" +
                "END;"
        connection.createStatement().execute(sql)
        connection.close()
    }

    def "get schemas list"() {

        prepareStarsTable();

        when:
        def result = runSchemasList(getStarsConfig())
        then:
        result.contains("ELASTICIO")
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
        result.toString() == "{\"in\":{\"type\":\"object\",\"properties\":{\"P_CUS_ID\":{\"type\":\"number\",\"name\":\"P_CUS_ID\",\"required\":true},\"O_NAME\":{\"type\":\"string\",\"name\":\"O_NAME\",\"required\":true}}},\"out\":{\"type\":\"object\",\"properties\":{\"O_NAME\":{\"type\":\"string\",\"name\":\"O_NAME\",\"required\":true},\"O_CITY\":{\"type\":\"string\",\"name\":\"O_CITY\",\"required\":true},\"O_DATE\":{\"type\":\"string\",\"name\":\"O_DATE\",\"required\":true}}}}"
    }
}
