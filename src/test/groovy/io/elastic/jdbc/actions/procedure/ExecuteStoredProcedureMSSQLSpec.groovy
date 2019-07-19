package io.elastic.jdbc.actions.procedure

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.ExecuteStoredProcedure
import org.junit.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class ExecuteStoredProcedureMSSQLSpec extends Specification {
    @Shared
    def user = System.getenv("CONN_USER_MSSQL")
    @Shared
    def password = System.getenv("CONN_PASSWORD_MSSQL")
    @Shared
    def databaseName = System.getenv("CONN_DBNAME_MSSQL")
    @Shared
    def host = System.getenv("CONN_HOST_MSSQL")
    @Shared
    def port = System.getenv("CONN_PORT_MSSQL")
    @Shared
    def connectionString = "jdbc:sqlserver://" + host + ":" + port + ";database=" + databaseName

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
                .add("schemaName", "dbo")
                .add("procedureName", "get_customer_by_id_and_name")
                .add("user", user)
                .add("password", password)
                .add("dbEngine", "mssql")
                .add("host", host)
                .add("port", port)
                .add("databaseName", databaseName)
                .build();
        return config;
    }

    def prepareStarsTable() {
        connection.createStatement().execute("DROP TABLE IF EXISTS CUSTOMERS")

        connection.createStatement().execute("create table CUSTOMERS (\n" +
                "PID INT NOT NULL,\n" +
                "NAME varchar(128),\n" +
                "CITY varchar(256),\n" +
                "JOINDATE datetime)");

        connection.createStatement().execute("create procedure GET_CUSTOMER_BY_ID_AND_NAME\n" +
                "   @p_cus_id int,\n" +
                "   @o_name varchar OUTPUT,\n" +
                "   @o_city varchar OUTPUT,\n" +
                "   @o_date datetime OUTPUT\n" +
                "AS\n" +
                "BEGIN\n" +
                "   SELECT @o_name = NAME, @o_city = CITY, @o_date = JOINDATE\n" +
                "   from  CUSTOMERS\n" +
                "   WHERE PID = @p_cus_id\n" +
                "   AND NAME = @o_name;\n" +
                "END;");

        connection.createStatement().execute("INSERT INTO CUSTOMERS (PID,NAME,CITY,JOINDATE) VALUES (1,'Alice','Kyiv','2019-07-09T09:20:30Z')")
        connection.createStatement().execute("INSERT INTO CUSTOMERS (PID,NAME,CITY,JOINDATE) VALUES (2,'Bob','London','2019-07-09T09:20:31Z')")
    }

    def cleanupSpec() {
        connection.createStatement().execute("DROP TABLE IF EXISTS CUSTOMERS")
        connection.createStatement().execute("DROP PROCEDURE GET_CUSTOMER_BY_ID_AND_NAME")
        connection.close()
    }

    def "call procedure"() {

        prepareStarsTable();

        JsonObject snapshot = Json.createObjectBuilder().build()

        JsonObject body = Json.createObjectBuilder()
                .add("@p_cus_id", 2)
                .add("@o_name", "Bob")
                .add("@o_city", "")
                .add("@o_date", "")
                .build();

        when:
        runAction(getStarsConfig(), body, snapshot)
        then:
        1 * dataCallback.receive(_)
        0 * errorCallback.receive(_)
    }
}