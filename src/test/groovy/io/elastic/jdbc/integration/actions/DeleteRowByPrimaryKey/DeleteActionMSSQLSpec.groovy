package io.elastic.jdbc.integration.actions.DeleteRowByPrimaryKey

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.DeleteRowByPrimaryKey
import org.junit.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

@Ignore
class DeleteActionMSSQLSpec extends Specification {

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
  EventEmitter.Callback httpReplyCallback
  @Shared
  EventEmitter emitter
  @Shared
  DeleteRowByPrimaryKey action

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
    action = new DeleteRowByPrimaryKey()
  }

  def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
    Message msg = new Message.Builder().body(body).build()
    ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
    action.execute(params);
  }

  def getStarsConfig() {
    JsonObject config = Json.createObjectBuilder()
        .add("tableName", "stars")
        .add("user", user)
        .add("password", password)
        .add("dbEngine", "mssql")
        .add("host", host)
        .add("port", port)
        .add("databaseName", databaseName)
        .add("nullableResult", "true")
        .build();
    return config;
  }

  def prepareStarsTable() {
    connection.createStatement().execute("IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;");
    connection.createStatement().execute("CREATE TABLE stars (id int PRIMARY KEY, name varchar(255) NOT NULL, " +
        "date datetime, radius int, destination int, visible bit, visibledate date)");
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
    String sql = "IF OBJECT_ID('persons', 'U') IS NOT NULL\n" +
        "  DROP TABLE persons;"

    connection.createStatement().execute(sql)
    sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql)
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
