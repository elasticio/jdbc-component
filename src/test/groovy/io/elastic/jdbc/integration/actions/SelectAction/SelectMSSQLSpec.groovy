package io.elastic.jdbc.integration.actions.SelectAction

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.SelectAction
import spock.lang.*

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class SelectMSSQLSpec extends Specification {
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
  EventEmitter.Callback onHttpReplyCallback
  @Shared
  EventEmitter emitter
  @Shared
  SelectAction action

  def setupSpec() {
    connection = DriverManager.getConnection(connectionString, user, password)
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
    JsonObject config = Json.createObjectBuilder()
        .add("user", user)
        .add("password", password)
        .add("dbEngine", "mssql")
        .add("host", host)
        .add("port", port)
        .add("databaseName", databaseName)
        .add("sqlQuery", "SELECT * from stars")
        .build();
    return config;
  }

  def prepareStarsTable() {
    String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql);
    connection.createStatement().execute("CREATE TABLE stars (id int, name varchar(255) NOT NULL, date datetime, radius int, destination int)");
    connection.createStatement().execute("INSERT INTO stars (id, name) VALUES (1,'Hello')");
  }

  def cleanupSpec() {
    String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "one select"() {

    prepareStarsTable();
    JsonObject snapshot = Json.createObjectBuilder().build();
    JsonObject body = Json.createObjectBuilder().build()

    when:
    runAction(getStarsConfig(), body, snapshot)
    then:
    0 * errorCallback.receive(_)
  }
}
