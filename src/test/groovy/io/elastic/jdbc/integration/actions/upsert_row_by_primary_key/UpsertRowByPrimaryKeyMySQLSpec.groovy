package io.elastic.jdbc.integration.actions.upsert_row_by_primary_key

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.UpsertRowByPrimaryKey
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class UpsertRowByPrimaryKeyMySQLSpec extends Specification {

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
  UpsertRowByPrimaryKey action

  def setupSpec() {
    JsonObject config = getStarsConfig();
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
    action = new UpsertRowByPrimaryKey()
  }

  def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
    Message msg = new Message.Builder().body(body).build()
    ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
    action.execute(params);
  }

  def getStarsConfig() {
    JsonObject config = TestUtils.getMysqlConfigurationBuilder()
    .add("tableName", "stars")
    .build();
    return config;
  }

  def prepareStarsTable() {

    String sql = "DROP TABLE IF EXISTS stars;"
    connection.createStatement().execute(sql);
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
    String sql = "DROP TABLE IF EXISTS persons;"

    connection.createStatement().execute(sql)
    sql = "DROP TABLE IF EXISTS stars;"
    connection.createStatement().execute(sql)
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

    runAction(getStarsConfig(), body, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 1
    records.get(0) == '{id=1, name=Taurus, date=2015-02-19 10:10:10.0, radius=123, destination=null, visible=true, ' +
            'visibledate=null}'
  }

  def "one insert, incorrect value: string in integer field"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
    .add("id", "1")
    .add("name", "Taurus")
    .add("radius", "test")
    .build()
    String exceptionClass = "";

    try {
      runAction(getStarsConfig(), body, snapshot)
    } catch (Exception e) {
      exceptionClass = e.getClass().getName();
    }

    expect:
    exceptionClass.contains("Exception")
  }

  def "two inserts"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("id", 1)
    .add("name", "Taurus")
    .add("radius", 123)
    .build()
    runAction(getStarsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("id", 2)
    .add("name", "Eridanus")
    .add("radius", 456)
    .build()

    runAction(getStarsConfig(), body2, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 2
    records.get(0) == '{id=1, name=Taurus, date=null, radius=123, destination=null, visible=null, visibledate=null}'
    records.get(1) == '{id=2, name=Eridanus, date=null, radius=456, destination=null, visible=null, visibledate=null}'
  }

  def "one insert, one update by ID"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("id", 1)
    .add("name", "Taurus")
    .add("radius", 123)
    .build()
    runAction(getStarsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("id", 1)
    .add("name", "Eridanus")
    .build()
    runAction(getStarsConfig(), body2, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 1
    records.get(0) == '{id=1, name=Eridanus, date=null, radius=123, destination=null, visible=null, visibledate=null}'
  }


  def getPersonsConfig() {
    JsonObject config = TestUtils.getMysqlConfigurationBuilder()
    .add("tableName", "persons")
    .build()
    return config
  }

  def preparePersonsTable() {
    String sql = "DROP TABLE IF EXISTS persons;"
    connection.createStatement().execute(sql);
    connection.createStatement().execute("CREATE TABLE persons (id int, name varchar(255) NOT NULL, " +
            "email varchar(255) NOT NULL PRIMARY KEY)");
  }

  def "one insert, name with quote"() {

    preparePersonsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("id", 1)
    .add("name", "O'Henry")
    .add("email", "ohenry@elastic.io")
    .build()
    runAction(getPersonsConfig(), body1, snapshot)

    ArrayList<String> records = getRecords("persons")

    expect:
    records.size() == 1
    records.get(0) == '{id=1, name=O\'Henry, email=ohenry@elastic.io}'
  }

  def "two inserts, one update by email"() {

    preparePersonsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("id", 1)
    .add("name", "User1")
    .add("email", "user1@elastic.io")
    .build()
    runAction(getPersonsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("id", 2)
    .add("name", "User2")
    .add("email", "user2@elastic.io")
    .build()
    runAction(getPersonsConfig(), body2, snapshot)

    JsonObject body3 = Json.createObjectBuilder()
    .add("id", 3)
    .add("name", "User3")
    .add("email", "user2@elastic.io")
    .build()
    runAction(getPersonsConfig(), body3, snapshot)

    ArrayList<String> records = getRecords("persons")

    expect:
    records.size() == 2
    records.get(0) == '{id=1, name=User1, email=user1@elastic.io}'
    records.get(1) == '{id=3, name=User3, email=user2@elastic.io}'
  }
}
