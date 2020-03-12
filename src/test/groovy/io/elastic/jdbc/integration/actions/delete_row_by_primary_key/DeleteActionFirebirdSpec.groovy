package io.elastic.jdbc.integration.actions.delete_row_by_primary_key

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.DeleteRowByPrimaryKey
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class DeleteActionFirebirdSpec extends Specification {
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
  DeleteRowByPrimaryKey action

  def setupSpec() {
    JsonObject config = getStarsConfig()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
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
    action = new DeleteRowByPrimaryKey()
  }

  def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
    Message msg = new Message.Builder().body(body).build()
    ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
    action.execute(params);
  }

  def getStarsConfig() {
    JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
        .add("tableName", "stars")
        .add("nullableResult", "true")
        .build();
    return config;
  }

  def prepareStarsTable() {
    connection.createStatement().execute("CREATE TABLE stars (id int PRIMARY KEY, name varchar(255) NOT NULL, " +
        "datet timestamp, radius int, destination int, visible smallint, visibledate date)");
    connection.createStatement().execute("INSERT INTO stars values (1,'Taurus', '2015-02-19 10:10:10.0'," +
        " 123, 5, 0, '2015-02-19')")
    connection.createStatement().execute("INSERT INTO stars values (2,'Eridanus', '2017-02-19 10:10:10.0'," +
        " 852, 5, 0, '2015-07-19')")
  }

  def getRecords(tableName) {
    ArrayList<String> records = new ArrayList<String>();
    String sql = "SELECT * FROM " + tableName;
    ResultSet rs = connection.createStatement().executeQuery(sql);
    while (rs.next()) {
      records.add(rs.toRowResult().toString());
    }
    rs.close();
    return records;
  }

  def cleanupSpec() {
    connection.createStatement().execute("DROP TABLE stars;")
    connection.close()
  }

  def "one delete"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
        .add("id", 1)
        .build();

    runAction(getStarsConfig(), body, snapshot)
    int first = getRecords("stars").size()
    JsonObject body2 = Json.createObjectBuilder()
        .add("id", 2)
        .build()
    runAction(getStarsConfig(), body2, snapshot)
    int second = getRecords("stars").size()

    expect:
    first == 1
    second == 0
  }
}
