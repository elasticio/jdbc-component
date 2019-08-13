package io.elastic.jdbc.integration.triggers.SelectTriggerOld

import com.google.gson.JsonObject
import io.elastic.api.EventEmitter
import io.elastic.api.EventEmitter.Callback
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.SailorVersionsAdapter
import io.elastic.jdbc.triggers.SelectTriggerOld
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

@Deprecated
@Ignore
class SelectTriggerOldOracleOldSpec extends Specification {

    @Shared def connectionString = ""
    @Shared def user = ""
    @Shared def password = ""
    @Shared Connection connection

    def setup() {
        connection = DriverManager.getConnection(connectionString, user, password)

        String sql = "BEGIN" +
                "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
                "EXCEPTION" +
                "   WHEN OTHERS THEN" +
                "      IF SQLCODE != -942 THEN" +
                "         RAISE;" +
                "      END IF;" +
                "END;"
        connection.createStatement().execute(sql)

        sql = "CREATE TABLE stars (ID number, name varchar(255) NOT NULL, radius number, destination float)"
        connection.createStatement().execute(sql)

        sql = "INSERT INTO stars (ID, name, radius, destination) VALUES (1, 'Sun', 50, 170)"
        connection.createStatement().execute(sql)
    }

    def cleanup() {
        connection.close();
    }

    def "make a SELECT request" () {

        Callback errorCallback = Mock(Callback)
        Callback snapshotCallback = Mock(Callback)
        Callback dataCallback = Mock(Callback)
        Callback onreboundCallback = Mock(Callback)
        Callback httpReplyCallback = Mock(Callback)

        EventEmitter emitter = new EventEmitter.Builder()
                .onData(dataCallback)
                .onSnapshot(snapshotCallback)
                .onError(errorCallback)
                .onRebound(onreboundCallback)
                .onHttpReplyCallback(httpReplyCallback).build();

        SelectTriggerOld selectAction = new SelectTriggerOld();

        given:
        Message msg = new Message.Builder().build();

        JsonObject config = new JsonObject()
        config.addProperty("orderField", "name")
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "oracle")
        config.addProperty("host", "")
        config.addProperty("databaseName", "ORCL")
        config.addProperty("tableName", "stars")

        JsonObject snapshot = new JsonObject()
        snapshot.addProperty("skipNumber", 0)

        when:
        ExecutionParameters params = new ExecutionParameters(msg, emitter,
                SailorVersionsAdapter.gsonToJavax(config), SailorVersionsAdapter.gsonToJavax(snapshot))
        selectAction.execute(params)

        then:
        0 * errorCallback.receive(_)
        1 * dataCallback.receive({ it.toString() =='{"body":{"ID":"1","NAME":"Sun","RADIUS":"50","DESTINATION":"170","RNK":"1"},"attachments":{}}' })
        1 * snapshotCallback.receive({ it.toString() == '{"skipNumber":1,"tableName":"stars"}'})
    }
}
