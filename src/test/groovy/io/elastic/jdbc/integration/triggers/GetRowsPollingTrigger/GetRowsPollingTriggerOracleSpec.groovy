package io.elastic.jdbc.integration.triggers.GetRowsPollingTrigger

import io.elastic.api.EventEmitter
import io.elastic.api.EventEmitter.Callback
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.triggers.GetRowsPollingTrigger
import spock.lang.*

import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class GetRowsPollingTriggerOracleSpec extends Specification {

  @Shared
  def connectionString = System.getenv("CONN_URI_ORACLE")
  @Shared
  def user = System.getenv("CONN_USER_ORACLE")
  @Shared
  def password = System.getenv("CONN_PASSWORD_ORACLE")
  @Shared
  def databaseName = System.getenv("CONN_DBNAME_ORACLE")
  @Shared
  def host = System.getenv("CONN_HOST_ORACLE")

  @Shared
  Connection connection

  def setup() {
    connection = DriverManager.getConnection(connectionString, user, password)

    String sql = "BEGIN" +
        "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
        "EXCEPTION" +
        "   WHEN OTHERS THEN" +
        "      IF SQLCODE != -942 THEN" +
        "         RAISE;" +
        "      END IF;" +
        "END;"
    connection.createStatement().execute(sql)

    sql = "CREATE TABLE stars (ID number, name varchar(255) NOT NULL, radius number, destination float, createdat date)"
    connection.createStatement().execute(sql)

    sql = "INSERT INTO stars (ID, name, radius, destination, createdat) VALUES (1, 'Sun', 50, 170, TO_DATE('2018-06-14 10:00:00', 'yyyy-mm-dd hh24:mi:ss'))"
    connection.createStatement().execute(sql)
  }

  def cleanup() {
    connection.close();
  }

  def "make a SELECT request"() {

    Callback errorCallback = Mock(Callback)
    Callback snapshotCallback = Mock(Callback)
    Callback dataCallback = Mock(Callback)
    Callback onreboundCallback = Mock(Callback)

    EventEmitter emitter = new EventEmitter.Builder()
        .onData(dataCallback)
        .onSnapshot(snapshotCallback)
        .onError(errorCallback)
        .onRebound(onreboundCallback).build();

    GetRowsPollingTrigger getRowsPollingTrigger = new GetRowsPollingTrigger(emitter);

    given:
    Message msg = new Message.Builder().build();

    JsonObject config = Json.createObjectBuilder().build()
    config.addProperty("pollingField", "createdat")
    config.addProperty("pollingValue", "2018-06-14 00:00:00")
    config.addProperty("user", user)
    config.addProperty("password", password)
    config.addProperty("dbEngine", "oracle")
    config.addProperty("host", host)
    config.addProperty("databaseName", databaseName)
    config.addProperty("tableName", "stars")

    JsonObject snapshot = Json.createObjectBuilder().build()
    snapshot.addProperty("skipNumber", 0)

    when:
    ExecutionParameters params = new ExecutionParameters(msg, config, snapshot)
    getRowsPollingTrigger.execute(params)

    then:
    0 * errorCallback.receive(_)
    1 * dataCallback.receive({
      it.toString() == '{"body":{"ID":"1","NAME":"Sun","RADIUS":"50","DESTINATION":"170","CREATEDAT":"2018-06-14 10:00:00.0","RNK":"1"},"attachments":{}}'
    })
    1 * snapshotCallback.receive({
      it.toString() == '{"skipNumber":1,"tableName":"stars","pollingField":"createdat","pollingValue":"2018-06-14 10:00:00.000"}'
    })
  }
}