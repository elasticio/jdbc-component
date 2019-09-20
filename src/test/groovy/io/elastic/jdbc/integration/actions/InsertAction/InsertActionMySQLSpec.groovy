package io.elastic.jdbc.integration.actions.InsertAction

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.InsertAction
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class InsertActionMySQLSpec extends Specification {

  @Shared
  Connection connection
  @Shared
  JsonObject config

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
  InsertAction action

  def setupSpec() {
    config = TestUtils.getMysqlConfigurationBuilder()
        .add("tableName", "stars")
        .build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"));
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
    action = new InsertAction()
  }

  def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
    Message msg = new Message.Builder().body(body).build()
    ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
    action.execute(params);
  }


  def prepareStarsTable() {

    connection.createStatement().execute("DROP TABLE IF EXISTS stars;");
    connection.createStatement().execute("CREATE TABLE stars (id int PRIMARY KEY, name varchar(255) NOT NULL, " +
        "date datetime, radius int, destination int, visible bit, visibledate date)");
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
    connection.createStatement().execute("DROP TABLE IF EXISTS stars;")
    connection.close()
  }

  def "one insert"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
        .add("id", 1)
        .add("name", "Taurus")
        .add("date", "2015-02-19 10:10:10.0")
        .add("radius", 123)
        .add("visible", true)
        .build();

    runAction(config, body, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 1
    records.get(0) == '{id=1, name=Taurus, date=2015-02-19 10:10:10.0, radius=123, destination=null, visible=true, ' +
        'visibledate=null}'
  }
}
