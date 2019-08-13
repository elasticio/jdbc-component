package io.elastic.jdbc.integration.actions.UpsertRowByPrimaryKey

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.UpsertRowByPrimaryKey
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

@Ignore
class UpsertRowByPrimaryKeyOracleSpec extends Specification {

  @Shared
  def user = System.getenv("CONN_USER_ORACLE")
  @Shared
  def password = System.getenv("CONN_PASSWORD_ORACLE")
  @Shared
  def databaseName = System.getenv("CONN_DBNAME_ORACLE")
  @Shared
  def host = System.getenv("CONN_HOST_ORACLE")
  @Shared
  def port = System.getenv("CONN_PORT_ORACLE")
  @Shared
  def dbEngine = "oracle"
  @Shared
  def connectionString ="jdbc:oracle:thin:@//" + host + ":" + port + "/XE"
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
    connection = DriverManager.getConnection(connectionString, user, password)
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
    JsonObject config = Json.createObjectBuilder()
    .add("tableName", "STARS")
    .add("user", user)
    .add("password", password)
    .add("dbEngine", dbEngine)
    .add("host", host)
    .add("port", port)
    .add("databaseName", databaseName)
    .build();
    return config;
  }

  def prepareStarsTable() {
    String sql = "BEGIN" +
            "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
            "EXCEPTION" +
            "   WHEN OTHERS THEN" +
            "      IF SQLCODE != -942 THEN" +
            "         RAISE;" +
            "      END IF;" +
            "END;"
    connection.createStatement().execute(sql);
    connection.createStatement().execute("CREATE TABLE stars (id number, name varchar(255) NOT NULL, " +
            "ndate timestamp, radius number, destination float,visible number(1), " +
            "CONSTRAINT pk_stars PRIMARY KEY (id))");
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
    String sql = "BEGIN" +
            "   EXECUTE IMMEDIATE 'DROP TABLE persons';" +
            "EXCEPTION" +
            "   WHEN OTHERS THEN" +
            "      IF SQLCODE != -942 THEN" +
            "         RAISE;" +
            "      END IF;" +
            "END;"

    connection.createStatement().execute(sql)
    sql = "BEGIN" +
            "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
            "EXCEPTION" +
            "   WHEN OTHERS THEN" +
            "      IF SQLCODE != -942 THEN" +
            "         RAISE;" +
            "      END IF;" +
            "END;"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "one insert"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
    .add("ID", 1)
    .add("name", "Taurus")
    .add("ndate", "2015-02-19 10:10:10")
    .add("radius", 123)
    .add("visible", 1)
    .build();

    runAction(getStarsConfig(), body, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 1
    records.get(0) == '{ID=1, NAME=Taurus, NDATE=2015-02-19 10:10:10.0, RADIUS=123, DESTINATION=null, VISIBLE=1}'
  }

  def "one insert, incorrect value: string in integer field"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
    .add("ID", 1)
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
    .add("ID", 1)
    .add("name", "Taurus")
    .add("radius", 123)
    .build()
    runAction(getStarsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("ID", 2)
    .add("name", "Eridanus")
    .add("radius", 456)
    .build()

    runAction(getStarsConfig(), body2, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 2
    records.get(0) == '{ID=1, NAME=Taurus, NDATE=null, RADIUS=123, DESTINATION=null, VISIBLE=null}'
    records.get(1) == '{ID=2, NAME=Eridanus, NDATE=null, RADIUS=456, DESTINATION=null, VISIBLE=null}'
  }

  def "one insert, one update by ID"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("ID", 1)
    .add("name", "Taurus")
    .add("radius", 123)
    .build()
    runAction(getStarsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("ID", 1)
    .add("name", "Eridanus")
    .build()
    runAction(getStarsConfig(), body2, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 1
    records.get(0) == '{ID=1, NAME=Eridanus, NDATE=null, RADIUS=123, DESTINATION=null, VISIBLE=null}'
  }


  def getPersonsConfig() {
    JsonObject config = Json.createObjectBuilder()
    .add("tableName", "PERSONS")
    .add("user", user)
    .add("password", password)
    .add("dbEngine", dbEngine)
    .add("host", host)
    .add("port", port)
    .add("databaseName", databaseName)
    .build()
    return config
  }

  def preparePersonsTable() {
    String sql = "BEGIN" +
            "   EXECUTE IMMEDIATE 'DROP TABLE persons';" +
            "EXCEPTION" +
            "   WHEN OTHERS THEN" +
            "      IF SQLCODE != -942 THEN" +
            "         RAISE;" +
            "      END IF;" +
            "END;"
    connection.createStatement().execute(sql);
    connection.createStatement().execute("CREATE TABLE persons (id int, name varchar(255) NOT NULL, " +
            "EMAIL varchar(255) NOT NULL, CONSTRAINT pk_persons PRIMARY KEY (EMAIL))");
  }

  def "one insert, name with quote"() {

    preparePersonsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("id", 1)
    .add("name", "O'Henry")
    .add("EMAIL", "ohenry@elastic.io")
    .build()
    runAction(getPersonsConfig(), body1, snapshot)

    ArrayList<String> records = getRecords("persons")

    expect:
    records.size() == 1
    records.get(0) == '{ID=1, NAME=O\'Henry, EMAIL=ohenry@elastic.io}'
  }

  def "two inserts, one update by email"() {

    preparePersonsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder()
    .add("id", 1)
    .add("name", "User1")
    .add("EMAIL", "user1@elastic.io")
    .build()
    runAction(getPersonsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder()
    .add("id", 2)
    .add("name", "User2")
    .add("EMAIL", "user2@elastic.io")
    .build()
    runAction(getPersonsConfig(), body2, snapshot)

    JsonObject body3 = Json.createObjectBuilder()
    .add("id", 3)
    .add("name", "User3")
    .add("EMAIL", "user2@elastic.io")
    .build()
    runAction(getPersonsConfig(), body3, snapshot)

    ArrayList<String> records = getRecords("persons")

    expect:
    records.size() == 2
    records.get(0) == '{ID=1, NAME=User1, EMAIL=user1@elastic.io}'
    records.get(1) == '{ID=3, NAME=User3, EMAIL=user2@elastic.io}'
  }
}
