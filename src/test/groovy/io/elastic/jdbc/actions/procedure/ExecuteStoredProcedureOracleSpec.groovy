package io.elastic.jdbc.actions.procedure

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.ExecuteStoredProcedure
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class ExecuteStoredProcedureOracleSpec extends Specification {
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
    def connectionString = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + databaseName
    @Shared
    Connection connection

    @Shared
    EventEmitter.Callback errorCallback
    @Shared
    EventEmitter.Callback snapshotCallback
    @Shared
    EventEmitter.Callback dataCallback
    @Shared
    EventEmitter.Callback reboundCallback
    @Shared
    EventEmitter.Callback httpReplyCallback
    @Shared
    EventEmitter emitter
    @Shared
    ExecuteStoredProcedure action

    def setupSpec() {
        connection = DriverManager.getConnection(connectionString, user, password)
    }

    def setup() {
        createAction()
    }

    def createAction() {
        errorCallback = Mock(EventEmitter.Callback)
        snapshotCallback = Mock(EventEmitter.Callback)
        dataCallback = Mock(EventEmitter.Callback)
        reboundCallback = Mock(EventEmitter.Callback)
        httpReplyCallback = Mock(EventEmitter.Callback)
        emitter = new EventEmitter.Builder().onData(dataCallback).onSnapshot(snapshotCallback).onError(errorCallback)
                .onRebound(reboundCallback).onHttpReplyCallback(httpReplyCallback).build()
        action = new ExecuteStoredProcedure()
    }

    def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
        Message msg = new Message.Builder().body(body).build()
        ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
        action.execute(params);
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

    def "call procedure"() {

        prepareStarsTable();

        JsonObject snapshot = Json.createObjectBuilder().build()

        JsonObject body = Json.createObjectBuilder()
                .add("P_CUS_ID", 2)
                .add("O_NAME", "Bob")
                .build();

        when:
        runAction(getStarsConfig(), body, snapshot)
        then:
        1 * dataCallback.receive(_)
        0 * errorCallback.receive(_)
    }
}