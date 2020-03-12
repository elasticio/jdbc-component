package io.elastic.jdbc.integration.triggers.select_trigger

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.triggers.SelectTrigger
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class SelectTriggerFirebirdSpec extends Specification {
    @Shared
    def credentials = TestUtils.getFirebirdConfigurationBuilder().build()
    @Shared
    Connection connection
    @Shared
    JsonObject configuration

    @Shared
    EventEmitter.Callback errorCallback
    @Shared
    EventEmitter.Callback snapshotCallback
    @Shared
    EventEmitter.Callback dataCallback
    @Shared
    EventEmitter.Callback reboundCallback
    @Shared
    EventEmitter.Callback onHttpReplyCallback
    @Shared
    EventEmitter emitter
    @Shared
    SelectTrigger trigger

    def setupSpec() {
        configuration = TestUtils.getFirebirdConfigurationBuilder()
                .add("tableName", TestUtils.TEST_TABLE_NAME)
                .build()
        connection = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"));
    }

    def setup() {
        trigger = new SelectTrigger()
    }

    def runTrigger(JsonObject config, JsonObject body, JsonObject snapshot) {
        Message msg = new Message.Builder().body(body).build()
        errorCallback = Mock(EventEmitter.Callback)
        snapshotCallback = Mock(EventEmitter.Callback)
        dataCallback = Mock(EventEmitter.Callback)
        reboundCallback = Mock(EventEmitter.Callback)
        onHttpReplyCallback = Mock(EventEmitter.Callback)
        emitter = new EventEmitter.Builder()
                .onData(dataCallback)
                .onSnapshot(snapshotCallback)
                .onError(errorCallback)
                .onRebound(reboundCallback)
                .onHttpReplyCallback(onHttpReplyCallback).build()
        ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
        trigger.execute(params);
    }

    def getStarsConfig() {
        JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
                .add("sqlQuery", "SELECT * from stars where id = 1")
                .build();
        return config;
    }

    def prepareStarsTable() {
        connection.createStatement().execute("CREATE TABLE stars (id int, name varchar(255) NOT NULL, datet timestamp, radius int, destination int)");
        connection.createStatement().execute("INSERT INTO stars (id, name) VALUES (1,'Hello')");
    }

    def cleanupSpec() {
        String sql = "DROP TABLE stars;"
        connection.createStatement().execute(sql)
        connection.close()
    }

    def "one select"() {
        prepareStarsTable();
        JsonObject snapshot = Json.createObjectBuilder().build();
        JsonObject body = Json.createObjectBuilder().build()

        when:
        runTrigger(getStarsConfig(), body, snapshot)
        then:
        0 * errorCallback.receive(_)
    }
}
