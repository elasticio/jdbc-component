package io.elastic.jdbc.integration.actions.insert_action

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.InsertAction
import spock.lang.Shared
import spock.lang.Specification

import jakarta.json.Json
import jakarta.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

class InsertActionPostgreSqlSpec extends Specification {
  @Shared
  Connection connection
  @Shared
  JsonObject configuration
  @Shared
  EventEmitter emitter = TestUtils.getFakeEventEmitter(Mock(EventEmitter.Callback))
  @Shared
  InsertAction action = new InsertAction()
  @Shared
  String dbEngine = "postgresql"


  def setupSpec() {
    configuration = TestUtils.getPostgresqlConfigurationBuilder()
        .add("tableName", TestUtils.TEST_TABLE_NAME)
        .build()
    connection = DriverManager.getConnection(configuration.getString("connectionString"), configuration.getString("user"), configuration.getString("password"));
    TestUtils.createTestTable(connection, dbEngine)
  }

  def cleanupSpec() {
    TestUtils.deleteTestTable(connection, dbEngine)
    connection.close()
  }

  def getRecords(tableName) {
    ArrayList<String> records = new ArrayList<String>();
    String sql = "SELECT * FROM " + tableName;
    ResultSet rs = connection.createStatement().executeQuery(sql);
    while (rs.next()) {
      records.add(rs.toRowResult().toString());
      rs.toRowResult()
    }
    rs.close();
    return records;
  }

  def "one insert"() {
    JsonObject body = Json.createObjectBuilder()
        .add("id", 1)
        .add("name", "Taurus")
        .add("radius", 12)
        .add("visible", true)
        .add("createdat", "2015-02-19 10:10:10.0")
        .build();
    Message msg = new Message.Builder().body(body).build()
    ExecutionParameters params = new ExecutionParameters(msg, emitter, configuration, Json.createObjectBuilder().build())

    action.execute(params);

    ArrayList<String> records = getRecords(TestUtils.TEST_TABLE_NAME)

    expect:
    records.size() == 1
    records.get(0) == '{id=1, name=Taurus, radius=12, destination=null, visible=true, createdat=2015-02-19 10:10:10.0}'
  }
}
