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

  def setup() {
    JsonObject config = TestUtils.getFirebirdConfigurationBuilder().build()
    connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))

    String sql = "CREATE TABLE stars (id int, isDead smallint, name varchar(255) NOT NULL, radius int, destination float, createdat timestamp)"
    connection.createStatement().execute(sql)

    sql = "INSERT INTO stars (id, isDead, name, radius, destination, createdat) VALUES (1, 0, 'Sun', 50, 170, '2018-06-14 10:00:00')"
    connection.createStatement().execute(sql)
  }

  def cleanupSpec() {
    connection.createStatement().execute("DROP TABLE stars")
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
    config.add("pollingField", "createdat")
        .add("pollingValue", "2018-06-14 00:00:00")
        .add("tableName", "stars")

    JsonObjectBuilder snapshot = Json.createObjectBuilder()
    snapshot.add("skipNumber", 0)

    when:
    ExecutionParameters params = new ExecutionParameters(msg, emitter, config.build(), snapshot.build())
    getRowsPollingTrigger.execute(params)

    then:
    0 * errorCallback.receive(_)
    dataCallback.receive({
      it.body.getInt("id").equals(1)
      it.body.getBoolean("isDead").equals(0)
      it.body.getString("name").equals("Sun")
      it.body.getString("createdat").equals("2018-06-14 13:00:00.0")
    })
  }
}