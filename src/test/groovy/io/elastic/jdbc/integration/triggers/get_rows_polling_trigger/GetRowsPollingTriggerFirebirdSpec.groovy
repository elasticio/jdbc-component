package io.elastic.jdbc.integration.triggers.get_rows_polling_trigger

import io.elastic.api.EventEmitter
import io.elastic.api.EventEmitter.Callback
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.triggers.GetRowsPollingTrigger
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

class GetRowsPollingTriggerFirebirdSpec extends Specification {


  @Shared
  Connection connection;
  @Shared
  JsonObject config = TestUtils.getFirebirdConfigurationBuilder().build()
  @Shared
  String sqlDropTable = "DROP TABLE STARS"
  @Shared
  String sqlCreateTable = "RECREATE TABLE STARS (ID int, ISDEAD smallint, NAME varchar(255) NOT NULL, RADIUS int, DESTINATION float, CREATEDAT timestamp)"
  @Shared
  String sqlInsertTable = "INSERT INTO STARS (ID, ISDEAD, NAME, RADIUS, DESTINATION, CREATEDAT) VALUES (1, 0, 'Sun', 50, 170, '2018-06-14 10:00:00')"

  def setup() {
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
    connection.createStatement().execute(sqlCreateTable)
    connection.createStatement().execute(sqlInsertTable)
    connection.close()
  }

  def cleanupSpec() {
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
    connection.createStatement().execute(sqlDropTable)
    connection.close()
  }

  def "make a SELECT request"() {

    Callback errorCallback = Mock(Callback)
    Callback snapshotCallback = Mock(Callback)
    Callback dataCallback = Mock(Callback)
    Callback onreboundCallback = Mock(Callback)
    Callback onHttpCallback = Mock(Callback)

    EventEmitter emitter = new EventEmitter.Builder()
        .onData(dataCallback)
        .onSnapshot(snapshotCallback)
        .onError(errorCallback)
        .onHttpReplyCallback(onHttpCallback)
        .onRebound(onreboundCallback).build();

    GetRowsPollingTrigger getRowsPollingTrigger = new GetRowsPollingTrigger();

    given:
    Message msg = new Message.Builder().build();

    JsonObjectBuilder config = TestUtils.getFirebirdConfigurationBuilder()
    config.add("pollingField", "CREATEDAT")
        .add("pollingValue", "2018-06-14 00:00:00")
        .add("tableName", "STARS")

    JsonObjectBuilder snapshot = Json.createObjectBuilder()
    snapshot.add("skipNumber", 0)

    when:
    ExecutionParameters params = new ExecutionParameters(msg, emitter, config.build(), snapshot.build())
    getRowsPollingTrigger.execute(params)

    then:
    0 * errorCallback.receive(_)
    dataCallback.receive({
      it.body.getInt("ID").equals(1)
      it.body.getBoolean("ISDEAD").equals(0)
      it.body.getString("NAME").equals("Sun")
      it.body.getString("CREATEDAT").equals("2018-06-14 13:00:00.0")
    })
  }
}