package io.elastic.jdbc.integration.actions.upsert_row_by_primary_key

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.UpsertRowByPrimaryKey
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class UpsertRowByPrimaryFirebirdSpec extends Specification {

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
  @Shared
  JsonObject config = getStarsConfig();
  @Shared
  String sqlDropStarsTable = "DROP TABLE STARS"
  @Shared
  String sqlCreateStarsTable = "RECREATE TABLE stars (ID int PRIMARY KEY, NAME varchar(255) NOT NULL, DATET timestamp, RADIUS int, DESTINATION int, VISIBLE smallint, VISIBLEDATE date)"
  @Shared
  String sqlDropPersonsTable = "DROP TABLE PERSONS"
  @Shared
  String sqlCreatePersonsTable = "RECREATE TABLE PERSONS (ID int, NAME varchar(255) NOT NULL, EMAIL varchar(255) NOT NULL PRIMARY KEY)"

  def cleanupSpec() {
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
    connection.createStatement().execute(sqlDropStarsTable);
    connection.createStatement().execute(sqlDropPersonsTable);
    connection.close()
  }

  def setup() {
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
    connection.createStatement().execute(sqlCreateStarsTable);
    connection.createStatement().execute(sqlCreatePersonsTable);
    createAction()
  }
  def cleanup() {
    connection.close()
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
    JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
    .add("tableName", "STARS")
    .build();
    return config;
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

  def "one insert"() {

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
    .add("ID", 1)
    .add("NAME", "Taurus")
    .add("DATET", "2015-02-19 10:10:10.0")
    .add("RADIUS", 123)
    .add("VISIBLE", 1)
    .add("VISIBLEDATE", "2015-02-19")
    .build();

    runAction(getStarsConfig(), body, snapshot)

    ArrayList<String> records = getRecords("STARS")

    expect:
    records.size() == 1
    records.get(0) == '{ID=1, NAME=Taurus, DATET=2015-02-19 10:10:10.0, RADIUS=123, DESTINATION=null, VISIBLE=1, VISIBLEDATE=2015-02-19}'
  }

  def "one insert, incorrect value: string in integer field"() {

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
    .add("ID", 1)
    .add("NAME", "Taurus")
    .add("RADIUS", "test")
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

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("ID", 1)
    .add("NAME", "Taurus")
    .add("RADIUS", 123)
    .build()
    runAction(getStarsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("ID", 2)
    .add("NAME", "Eridanus")
    .add("RADIUS", 456)
    .build()

    runAction(getStarsConfig(), body2, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 2
    records.get(0) == '{ID=1, NAME=Taurus, DATET=null, RADIUS=123, DESTINATION=null, VISIBLE=null, VISIBLEDATE=null}'
    records.get(1) == '{ID=2, NAME=Eridanus, DATET=null, RADIUS=456, DESTINATION=null, VISIBLE=null, VISIBLEDATE=null}'
  }

  def "one insert, one update by ID"() {

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("ID", 1)
    .add("NAME", "Taurus")
    .add("RADIUS", 123)
    .build()
    runAction(getStarsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("ID", 1)
    .add("NAME", "Eridanus")
    .build()
    runAction(getStarsConfig(), body2, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 1
    records.get(0) == '{ID=1, NAME=Eridanus, DATET=null, RADIUS=123, DESTINATION=null, VISIBLE=null, VISIBLEDATE=null}'
  }


  def getPersonsConfig() {
    JsonObject config = TestUtils.getFirebirdConfigurationBuilder()
    .add("tableName", "PERSONS")
    .build()
    return config
  }

  def "one insert, name with quote"() {

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("ID", 1)
    .add("NAME", "O'Henry")
    .add("EMAIL", "ohenry@elastic.io")
    .build()
    runAction(getPersonsConfig(), body1, snapshot)

    ArrayList<String> records = getRecords("PERSONS")

    expect:
    records.size() == 1
    records.get(0) == '{ID=1, NAME=O\'Henry, EMAIL=ohenry@elastic.io}'
  }

  def "two inserts, one update by email"() {

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("ID", 1)
    .add("NAME", "User1")
    .add("EMAIL", "user1@elastic.io")
    .build()
    runAction(getPersonsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("ID", 2)
    .add("NAME", "User2")
    .add("EMAIL", "user2@elastic.io")
    .build()
    runAction(getPersonsConfig(), body2, snapshot)

    JsonObject body3 = Json.createObjectBuilder()
    .add("ID", 3)
    .add("NAME", "User3")
    .add("EMAIL", "user2@elastic.io")
    .build()
    runAction(getPersonsConfig(), body3, snapshot)

    ArrayList<String> records = getRecords("PERSONS")

    expect:
    records.size() == 2
    records.get(0) == '{ID=1, NAME=User1, EMAIL=user1@elastic.io}'
    records.get(1) == '{ID=3, NAME=User3, EMAIL=user2@elastic.io}'
  }
}
