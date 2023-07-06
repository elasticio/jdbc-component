package io.elastic.jdbc.integration.triggers.get_rows_polling_trigger

import io.elastic.api.EventEmitter
import io.elastic.api.EventEmitter.Callback
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.triggers.GetRowsPollingTrigger
import spock.lang.*

import jakarta.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class GetRowsPollingTriggerMSSQLSpec extends Specification {

  @Shared
  def connectionString = System.getenv("CONN_URI_MSSQL")
  @Shared
  def user = System.getenv("CONN_USER_MSSQL")
  @Shared
  def password = System.getenv("CONN_PASSWORD_MSSQL")
  @Shared
  def databaseName = System.getenv("CONN_DBNAME_MSSQL")
  @Shared
  def host = System.getenv("CONN_HOST_MSSQL")

  @Shared
  Connection connection

  def setup() {
    connection = DriverManager.getConnection(connectionString, user, password)

    String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql)

    sql = "CREATE TABLE stars (ID int, name varchar(255) NOT NULL, radius int, destination float, createdat DATETIME)"
    connection.createStatement().execute(sql)

    sql = "INSERT INTO stars (ID, name, radius, destination, createdat) VALUES (1, 'Sun', 50, 170, '2018-06-14 10:00:00')"
    connection.createStatement().execute(sql)
  }

  def cleanupSpec() {
    String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql)
    connection.close()
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
    config.addProperty("dbEngine", "mssql")
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
      it.toString() == '{"body":{"ID":"1","name":"Sun","radius":"50","destination":"170.0","createdat":"2018-06-14 10:00:00.0","RowNum":"1"},"attachments":{}}'
    })
    1 * snapshotCallback.receive({
      it.toString() == '{"skipNumber":1,"tableName":"stars","pollingField":"createdat","pollingValue":"2018-06-14 10:00:00.000"}'
    })
  }
}
