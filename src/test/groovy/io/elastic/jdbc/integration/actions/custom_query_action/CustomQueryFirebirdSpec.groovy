package io.elastic.jdbc.integration.actions.custom_query_action

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.CustomQuery
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class CustomQueryFirebirdSpec extends Specification {
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
    CustomQuery action
    @Shared
    String sqlCreateTable = "RECREATE TABLE STARS (ID int, NAME varchar(255) NOT NULL, DATET timestamp, RADIUS int, DESTINATION int, VISIBLE smallint, VISIBLEDATE date)"
    @Shared
    String sqlDropTable = "DROP TABLE STARS"


    def cleanupSpec() {
        JsonObject config = getConfig()
        Connection connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
        connection.createStatement().execute(sqlDropTable)
        connection.close()
    }

    def setup() {
        errorCallback = Mock(EventEmitter.Callback)
        snapshotCallback = Mock(EventEmitter.Callback)
        dataCallback = Mock(EventEmitter.Callback)
        reboundCallback = Mock(EventEmitter.Callback)
        httpReplyCallback = Mock(EventEmitter.Callback)
        emitter = new EventEmitter.Builder().onData(dataCallback).onSnapshot(snapshotCallback).onError(errorCallback)
                .onRebound(reboundCallback).onHttpReplyCallback(httpReplyCallback).build()
        action = new CustomQuery()
        prepareStarsTable()
    }

    def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
        Message msg = new Message.Builder().body(body).build()
        ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
        action.execute(params);

    }

    def prepareStarsTable() {
        JsonObject config = getConfig()
        Connection connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
        connection.createStatement().execute(sqlCreateTable)
        connection.createStatement().execute("INSERT INTO stars values (1,'Taurus', '2015-02-19 10:10:10.0', 123, 5, 0, '2015-02-19')")
        connection.createStatement().execute("INSERT INTO stars values (2,'Eridanus', '2017-02-19 10:10:10.0', 852, 5, 0, '2015-07-19')")
        connection.close()
    }


    def getRecords(tableName) {
        ArrayList<String> records = new ArrayList<String>();
        Connection connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM " + tableName + ";");
        while (rs.next()) {
            records.add(rs.toRowResult().toString());
        }
        rs.close();
        connection.close()
        return records;
    }

    def "make insert"() {
        JsonObject snapshot = Json.createObjectBuilder().build()
        JsonObject body = Json.createObjectBuilder()
                .add("query", "INSERT INTO stars values (3, 'Rastaban', '2015-02-19 10:10:10.0', 123, 5, 1, '2018-02-19');")
                .build();

        when:
        runAction(getConfig(), body, snapshot)
        then:
        0 * errorCallback.receive(_)
        1 * dataCallback.receive({ it.getBody().getInt("updated") == 1 })

        int records = getRecords("STARS").size()
        expect:
        records == 3
    }

    def "make select"() {
        JsonObject snapshot = Json.createObjectBuilder().build()

        JsonObject body = Json.createObjectBuilder()
                .add("query", "SELECT * FROM STARS;")
                .build();

        when:
        runAction(getConfig(), body, snapshot)
        then:
        0 * errorCallback.receive(_)
        2 * dataCallback.receive({
            JsonObject msgBody = it.getBody()
            if (msgBody.getJsonArray("result") != null) {
                msgBody.getJsonArray("result").size() == 2
            } else {
                msgBody.getInt("updated") == 0
            }
        }
        )

    }

    def "make delete"() {
        JsonObject snapshot = Json.createObjectBuilder().build()

        JsonObject body = Json.createObjectBuilder()
                .add("query", "DELETE FROM STARS WHERE ID = 1;")
                .build();

        when:
        runAction(getConfig(), body, snapshot)
        then:
        0 * errorCallback.receive(_)
        1 * dataCallback.receive({ it.getBody().getInt("updated") == 1 })

        int records = getRecords("STARS").size()
        expect:
        records == 1
    }

    def getConfig() {
        JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
                .add("tableName", "STARS")
                .add("nullableResult", "true")
                .build();
        return config;
    }
}
