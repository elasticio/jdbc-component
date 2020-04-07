package io.elastic.jdbc.integration.actions.select_action

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.SelectAction
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class SelectFirebirdSpec extends Specification {

  @Shared
  String dbEngine = "firebirdsql"
  @Shared
  Connection connection
  @Shared
  JsonObject configuration = TestUtils.getFirebirdConfigurationBuilder()
          .add("tableName", TestUtils.TEST_TABLE_NAME)
          .build()
  @Shared
  EventEmitter.Callback errorCallback
  @Shared
  EventEmitter.Callback snapshotCallback
  @Shared
  EventEmitter.Callback dataCallback
  @Shared
  EventEmitter.Callback onHttpReplyCallback
  @Shared
  EventEmitter.Callback reboundCallback
  @Shared
  EventEmitter emitter
  @Shared
  SelectAction action

  def setupSpec() {
    connection = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"))
    TestUtils.createTestTable(connection, dbEngine)
    connection.createStatement().execute("INSERT INTO stars (id, name, radius, DESTINATION, visible, createdat) VALUES (1,'Hello', 1, 20, 0, '2015-02-19 10:10:10.0')");
    connection.createStatement().execute("INSERT INTO stars (id, name, radius, DESTINATION, visible, createdat) VALUES (2,'World', 1, 30, 1, '2015-02-19 10:10:10.0')");
  }

  def setup() {
    action = new SelectAction()
  }

  def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
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
    action.execute(params);
  }

  def getStarsConfig() {
    JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
        .add("sqlQuery", "SELECT * FROM STARS WHERE @id:number =id AND name=@name")
        .build()
    return config;
  }

  def cleanupSpec() {
    connection.close()
    Connection deleteCon = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"))
    TestUtils.deleteTestTable(deleteCon, dbEngine)
  }

  def "one select"() {
    JsonObject snapshot = Json.createObjectBuilder().build();
    JsonObject body = Json.createObjectBuilder()
        .add("ID", 1)
        .add("NAME", "Hello")
        .build()
    when:
    runAction(getStarsConfig(), body, snapshot)
    then:
    0 * errorCallback.receive(_)
  }

}
