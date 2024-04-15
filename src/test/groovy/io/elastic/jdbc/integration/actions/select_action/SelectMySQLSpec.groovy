package io.elastic.jdbc.integration.actions.select_action

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.actions.NewSelectAction
import spock.lang.Shared
import spock.lang.Specification

import jakarta.json.Json
import jakarta.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

class SelectMySQLSpec extends Specification {

  @Shared
  def credentials = TestUtils.getMysqlConfigurationBuilder().build()
  @Shared
  def host = credentials.getString("host")
  @Shared
  def port = credentials.getString("port")
  @Shared
  def databaseName = credentials.getString("databaseName")
  @Shared
  def user = credentials.getString("user")
  @Shared
  def password = credentials.getString("password")
  @Shared
  def connectionString = "jdbc:mysql://" + host + ":" + port + "/" + databaseName

  @Shared
  Connection connection

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
  NewSelectAction action

  def setupSpec() {
    connection = DriverManager.getConnection(connectionString, user, password)
  }

  def setup() {
    createAction()
  }

  def createAction() {
    action = new NewSelectAction()
  }

  def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
    String passthrough = "{\n" +
            "  \"id\": \"a405c0da-cf68-4196-a6b7-ab58998bc632\",\n" +
            "  \"attachments\": {},\n" +
            "  \"body\": {\n" +
            "    \"EmpID\": 8,\n" +
            "    \"EmpName\": \"Alex Rid\",\n" +
            "    \"Designation\": \"QA\",\n" +
            "    \"Department\": \"IT\",\n" +
            "    \"JoiningDate\": \"2019-08-27\",\n" +
            "    \"ColumnTIMESTAMP\": \"2019-05-03 00:00:01.0\"\n" +
            "  },\n" +
            "  \"headers\": {},\n" +
            "  \"passthrough\": {\n" +
            "    \"step_3\": {\n" +
            "      \"id\": \"a405c0da-cf68-4196-a6b7-ab58998bc632\",\n" +
            "      \"attachments\": {},\n" +
            "      \"body\": {\n" +
            "        \"EmpID\": 8,\n" +
            "        \"EmpName\": \"Alex Rid\",\n" +
            "        \"Designation\": \"QA\",\n" +
            "        \"Department\": \"IT\",\n" +
            "        \"JoiningDate\": \"2019-08-27\",\n" +
            "        \"ColumnTIMESTAMP\": \"2019-05-03 00:00:01.0\"\n" +
            "      },\n" +
            "      \"headers\": {},\n" +
            "      \"stepId\": \"step_3\"\n" +
            "    },\n" +
            "    \"step_2\": {\n" +
            "      \"body\": {}\n" +
            "    },\n" +
            "    \"step_1\": {\n" +
            "      \"body\": [\n" +
            "        {\n" +
            "          \"a\": 1\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"stepId\": \"step_3\"\n" +
            "}";
    jakarta.json.JsonObject passthroughJson = Json.createReader(new StringReader(passthrough)).readObject();
    Message msg = new Message.Builder()
            .body(body)
            .passthrough(passthroughJson)
            .build()
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
    JsonObject config = TestUtils.getMysqlConfigurationBuilder()
//        .add("sqlQuery", "SELECT * from stars where @id:number =id AND name=@name")
            .add("sqlQuery", "SELECT * from stars")
            .build()
    return config;
  }

  def prepareStarsTable() {
    String sql = "DROP TABLE IF EXISTS stars"
    connection.createStatement().execute(sql);
    connection.createStatement().execute("CREATE TABLE stars (id int, name varchar(255) NOT NULL, date datetime, radius int, destination int)");
    connection.createStatement().execute("INSERT INTO stars (id, name) VALUES (1,'Hello')");
    connection.createStatement().execute("INSERT INTO stars (id, name) VALUES (2,'World')");
  }

  def cleanupSpec() {
    String sql = "DROP TABLE IF EXISTS stars"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "one select"() {
    prepareStarsTable();
//    String passthrough = "{\n" +
//            "  \"id\": \"a405c0da-cf68-4196-a6b7-ab58998bc632\",\n" +
//            "  \"attachments\": {},\n" +
//            "  \"body\": {\n" +
//            "    \"EmpID\": 8,\n" +
//            "    \"EmpName\": \"Alex Rid\",\n" +
//            "    \"Designation\": \"QA\",\n" +
//            "    \"Department\": \"IT\",\n" +
//            "    \"JoiningDate\": \"2019-08-27\",\n" +
//            "    \"ColumnTIMESTAMP\": \"2019-05-03 00:00:01.0\"\n" +
//            "  },\n" +
//            "  \"headers\": {},\n" +
//            "  \"passthrough\": {\n" +
//            "    \"step_3\": {\n" +
//            "      \"id\": \"a405c0da-cf68-4196-a6b7-ab58998bc632\",\n" +
//            "      \"attachments\": {},\n" +
//            "      \"body\": {\n" +
//            "        \"EmpID\": 8,\n" +
//            "        \"EmpName\": \"Alex Rid\",\n" +
//            "        \"Designation\": \"QA\",\n" +
//            "        \"Department\": \"IT\",\n" +
//            "        \"JoiningDate\": \"2019-08-27\",\n" +
//            "        \"ColumnTIMESTAMP\": \"2019-05-03 00:00:01.0\"\n" +
//            "      },\n" +
//            "      \"headers\": {},\n" +
//            "      \"stepId\": \"step_3\"\n" +
//            "    },\n" +
//            "    \"step_2\": {\n" +
//            "      \"body\": {}\n" +
//            "    },\n" +
//            "    \"step_1\": {\n" +
//            "      \"body\": [\n" +
//            "        {\n" +
//            "          \"a\": 1\n" +
//            "        }\n" +
//            "      ]\n" +
//            "    }\n" +
//            "  },\n" +
//            "  \"stepId\": \"step_3\"\n" +
//            "}";
//    jakarta.json.JsonObject jsonBody = Json.createReader(new StringReader(body)).readObject();
    jakarta.json.JsonObject jsonBody = Json.createReader(new StringReader("{}")).readObject();
    jakarta.json.JsonObject snapshot = Json.createObjectBuilder().build();
//    JsonObject body = Json.createObjectBuilder()
//        .add("id", 1)
//        .add("name", "Hello")
//        .build()
    when:
    runAction(getStarsConfig(), jsonBody, snapshot)
    then:
    0 * errorCallback.receive(_)
  }

}
