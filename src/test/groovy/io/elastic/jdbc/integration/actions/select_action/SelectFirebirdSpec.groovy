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
  @Shared
  String sqlDropTable = "EXECUTE BLOCK AS BEGIN\n" +
          "if (exists(select 1 from rdb\$relations where rdb\$relation_name = 'STARS')) then\n" +
          "execute statement 'DROP TABLE STARS;';\n" +
          "END"
  @Shared
  String sqlCreateTable = "EXECUTE BLOCK AS BEGIN\n" +
          "if (not exists(select 1 from rdb\$relations where rdb\$relation_name = 'STARS')) then\n" +
          "execute statement 'CREATE TABLE stars (ID int PRIMARY KEY, NAME varchar(255) NOT NULL, DATET timestamp, RADIUS int, DESTINATION int);';\n" +
          "END"

  def setupSpec() {
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
        .add("sqlQuery", "SELECT * FROM STARS WHERE @id:number =id AND name=@name")
        .build()
    return config;
  }

  def prepareStarsTable() {
    connection.createStatement().execute(sqlCreateTable);
    connection.createStatement().execute("INSERT INTO stars (id, name) VALUES (1,'Hello')");
    connection.createStatement().execute("INSERT INTO stars (id, name) VALUES (2,'World')");
  }

  def cleanupSpec() {
    connection.close()

    connection = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"));
    connection.createStatement().execute(sqlDropTable)
    connection.close()
  }

  def "one select"() {
    prepareStarsTable();
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
