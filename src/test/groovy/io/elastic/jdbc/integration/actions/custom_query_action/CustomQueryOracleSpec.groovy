package io.elastic.jdbc.integration.actions.custom_query_action

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.CustomQuery
import spock.lang.Shared
import spock.lang.Specification

import jakarta.json.Json
import jakarta.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class CustomQueryOracleSpec extends Specification {
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
  EventEmitter.Callback reboundCallback
  @Shared
  EventEmitter.Callback httpReplyCallback
  @Shared
  EventEmitter emitter
  @Shared
  CustomQuery action

  def setupSpec() {
    configuration = getConfig()
    connection = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"));
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
    action = new CustomQuery()
  }

  def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
    Message msg = new Message.Builder().body(body).build()
    ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
    action.execute(params);
  }

  def getConfig() {
    JsonObject config = TestUtils.getOracleConfigurationBuilder()
            .add("nullableResult", "true")
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
            "radius number, destination float,visible number(1), " +
            "CONSTRAINT pk_stars PRIMARY KEY (id))");
    connection.createStatement().execute("INSERT INTO stars (ID,NAME,RADIUS,DESTINATION, VISIBLE) VALUES (1,'Taurus',321,44.4,1)")
    connection.createStatement().execute("INSERT INTO stars (ID,NAME,RADIUS,DESTINATION, VISIBLE) VALUES (2,'Boston',581,94.4,0)")
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

  def "make select"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
        .add("query", "SELECT * FROM stars")
        .build();

    when:
    runAction(getConfig(), body, snapshot)
    then:
    0 * errorCallback.receive(_)
    1 * dataCallback.receive({ it.getBody().getJsonArray("result").size() == 2 })
  }

  def "make insert"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
            .add("query", "INSERT INTO stars (ID,NAME,RADIUS,DESTINATION, VISIBLE) VALUES (3,'Rastaban', 123, 5, 1)")
            .build();

    when:
    runAction(getConfig(), body, snapshot)
    then:
    0 * errorCallback.receive(_)
    1 * dataCallback.receive({ it.getBody().getInt("updated") == 1 })

    int records = getRecords("stars").size()
    expect:
    records == 3
  }

  def "make delete"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body = Json.createObjectBuilder()
            .add("query", "DELETE FROM stars WHERE id = 1")
            .build();

    when:
    runAction(getConfig(), body, snapshot)
    then:
    0 * errorCallback.receive(_)
    1 * dataCallback.receive({ it.getBody().getInt("updated") == 1 })

    int records = getRecords("stars").size()
    expect:
    records == 1
  }

  def "successful transaction"() {
      prepareStarsTable();

      JsonObject snapshot = Json.createObjectBuilder().build()

      JsonObject body = Json.createObjectBuilder()
              .add("query", "BEGIN\n" +
              " DELETE FROM stars WHERE id = 1;\n" +
              " UPDATE stars SET radius = 5 WHERE id = 2;\n" +
              "end;")
              .build();

      when:
      runAction(getConfig(), body, snapshot)
      then:
      0 * errorCallback.receive(_)
      1 * dataCallback.receive({ it.getBody().getInt("updated") == -1 })

      int records = getRecords("stars").size()
      expect:
      records == 1
  }

  def "failed transaction"() {
      prepareStarsTable();

      JsonObject snapshot = Json.createObjectBuilder().build()

      JsonObject body = Json.createObjectBuilder()
              .add("query", "BEGIN\n" +
              " DELETE FROM stars WHERE id = 1;\n" +
              " UPDATE wrong_stars SET radius = 5 WHERE id = 2;\n" +
              "END;")
              .build();

      when:
      runAction(getConfig(), body, snapshot)
      then:
      RuntimeException e = thrown()
      e.message == 'java.sql.SQLException: ORA-06550: line 3, column 9:\n' +
              'PL/SQL: ORA-00942: table or view does not exist\n' +
              'ORA-06550: line 3, column 2:\n' +
              'PL/SQL: SQL Statement ignored\n'

      int records = getRecords("stars").size()
      expect:
      records == 2
  }
}
