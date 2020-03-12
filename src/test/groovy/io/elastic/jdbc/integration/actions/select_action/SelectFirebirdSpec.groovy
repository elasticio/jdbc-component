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
  EventEmitter.Callback onHttpReplyCallback
  @Shared
  EventEmitter.Callback reboundCallback
  @Shared
  EventEmitter emitter
  @Shared
  SelectAction action

  def setupSpec() {
    configuration = TestUtils.getFirebirdConfigurationBuilder()
            .add("tableName", TestUtils.TEST_TABLE_NAME)
            .build()
    connection = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"));
  }

  def setup() {
    createAction()
  }

  def createAction() {
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
        .add("sqlQuery", "SELECT * from stars where @id:number =id AND name=@name")
        .build()
    return config;
  }

  def prepareStarsTable() {
    connection.createStatement().execute("CREATE TABLE stars (id int, name varchar(255) NOT NULL, datet timestamp, radius int, destination int)");
    connection.createStatement().execute("INSERT INTO stars (id, name) VALUES (1,'Hello')");
    connection.createStatement().execute("INSERT INTO stars (id, name) VALUES (2,'World')");
  }

  def cleanupSpec() {
    String sql = "DROP TABLE stars"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "one select"() {
    prepareStarsTable();
    JsonObject snapshot = Json.createObjectBuilder().build();
    JsonObject body = Json.createObjectBuilder()
        .add("id", 1)
        .add("name", "Hello")
        .build()
    when:
    runAction(getStarsConfig(), body, snapshot)
    then:
    0 * errorCallback.receive(_)
  }

}
