package io.elastic.jdbc.integration.actions.create_or_update_record_deprecated
import com.google.gson.JsonObject
import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.CreateOrUpdateRecord
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

@Deprecated
@Ignore
class CreateOrUpdateRecordOracleOldSpec extends Specification {
	@Shared def connectionString = ""
	@Shared def user = ""
	@Shared def password = ""

	@Shared Connection connection

	@Shared EventEmitter.Callback errorCallback
	@Shared EventEmitter.Callback snapshotCallback
	@Shared EventEmitter.Callback dataCallback
	@Shared EventEmitter.Callback reboundCallback
	@Shared EventEmitter emitter
	@Shared CreateOrUpdateRecord action

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
		emitter = new EventEmitter.Builder().onData(dataCallback).onSnapshot(snapshotCallback).onError(errorCallback).onRebound(reboundCallback).build()
		action = new CreateOrUpdateRecord(emitter)
	}

	def runAction(JsonObject config, JsonObject body, JsonObject snapshot){
		Message msg = new Message.Builder().body(body).build()
		ExecutionParameters params = new ExecutionParameters(msg, config, snapshot)
		action.execute(params);
	}

	def getStarsConfig(){
		JsonObject config = new JsonObject()

		config.addProperty("idColumn", "id")
		config.addProperty("user", user)
		config.addProperty("password", password)
		config.addProperty("dbEngine", "oracle")
		config.addProperty("host", "")
		config.addProperty("databaseName", "ORCL")
		config.addProperty("tableName", "")
		return config;
	}

	def prepareStarsTable(){
		String sql = "BEGIN" +
				"   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
				"EXCEPTION" +
				"   WHEN OTHERS THEN" +
				"      IF SQLCODE != -942 THEN" +
				"         RAISE;" +
				"      END IF;" +
				"END;"
		connection.createStatement().execute(sql)
		connection.createStatement().execute("CREATE TABLE stars (id int, name varchar(255) NOT NULL, sdate TIMESTAMP, radius int, destination int)");
	}

	def getRecords(tableName){
		ArrayList<String> records = new ArrayList<String>();
		String sql = "SELECT * FROM " + tableName;
		ResultSet rs = connection.createStatement().executeQuery(sql);
		while(rs.next()){
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
		sql = "BEGIN" +
				"   EXECUTE IMMEDIATE 'DROP TABLE persons';" +
				"EXCEPTION" +
				"   WHEN OTHERS THEN" +
				"      IF SQLCODE != -942 THEN" +
				"         RAISE;" +
				"      END IF;" +
				"END;"
		connection.createStatement().execute(sql)
		connection.close()
	}

	def "one insert" () {

		prepareStarsTable();

		JsonObject snapshot = new JsonObject()

		JsonObject body = new JsonObject()
		body.addProperty("id", "1")
		body.addProperty("name", "Taurus")
		body.addProperty("radius", "123")
		body.addProperty("sdate", "2015-11-13 10:10:10")
		runAction(getStarsConfig(), body, snapshot)

		ArrayList<String> records = getRecords("stars")

		expect:
		records.size() == 1
		records.get(0) == '{ID=1, NAME=Taurus, SDATE=2015-11-13 10:10:10.0, RADIUS=123, DESTINATION=null}'
	}

	def "one insert, incorrect value: string in integer field" () {

		prepareStarsTable();

		JsonObject snapshot = new JsonObject()

		JsonObject body = new JsonObject()
		body.addProperty("id", "1")
		body.addProperty("name", "Taurus")
		body.addProperty("radius", "test")

		String exceptionClass = "";

		try {
			runAction(getStarsConfig(), body, snapshot)
		} catch (Exception e) {
			exceptionClass = e.getClass().getName();
		}

		expect:
		exceptionClass.contains('Exception')
	}

	def "two inserts" () {

		prepareStarsTable();

		JsonObject snapshot = new JsonObject()

		JsonObject body1 = new JsonObject()
		body1.addProperty("id", "1")
		body1.addProperty("name", "Taurus")
		body1.addProperty("radius", "123")

		runAction(getStarsConfig(), body1, snapshot)

		JsonObject body2 = new JsonObject()
		body2.addProperty("id", "2")
		body2.addProperty("name", "Eridanus")
		body2.addProperty("radius", "456")

		runAction(getStarsConfig(), body2, snapshot)

		ArrayList<String> records = getRecords("stars")

		expect:
		records.size() == 2
		records.get(0) == '{ID=1, NAME=Taurus, SDATE=null, RADIUS=123, DESTINATION=null}'
		records.get(1) == '{ID=2, NAME=Eridanus, SDATE=null, RADIUS=456, DESTINATION=null}'
	}

	def "one insert, one update by ID" () {

		prepareStarsTable();

		JsonObject snapshot = new JsonObject()

		JsonObject body1 = new JsonObject()
		body1.addProperty("id", "1")
		body1.addProperty("name", "Taurus")
		body1.addProperty("radius", "123")

		runAction(getStarsConfig(), body1, snapshot)

		JsonObject body2 = new JsonObject()
		body2.addProperty("id", "1")
		body2.addProperty("name", "Eridanus")

		runAction(getStarsConfig(), body2, snapshot)

		ArrayList<String> records = getRecords("stars")

		expect:
		records.size() == 1
		records.get(0) == '{ID=1, NAME=Eridanus, SDATE=null, RADIUS=123, DESTINATION=null}'
	}


	def getPersonsConfig(){
		JsonObject config = new JsonObject()
		config.addProperty("idColumn", "email")
		config.addProperty("user", user)
		config.addProperty("password", password)
		config.addProperty("dbEngine", "oracle")
		config.addProperty("host", "")
		config.addProperty("databaseName", "ORCL")
		config.addProperty("tableName", "persons")
		return config;
	}

	def preparePersonsTable(){
		String sql = "BEGIN" +
				"   EXECUTE IMMEDIATE 'DROP TABLE persons';" +
				"EXCEPTION" +
				"   WHEN OTHERS THEN" +
				"      IF SQLCODE != -942 THEN" +
				"         RAISE;" +
				"      END IF;" +
				"END;"
		connection.createStatement().execute(sql)
		connection.createStatement().execute("CREATE TABLE persons (id int, name varchar(255) NOT NULL, birth TIMESTAMP, email varchar(255) NOT NULL)");
	}

	def "one insert, name with quote" () {

		preparePersonsTable();

		JsonObject snapshot = new JsonObject()

		JsonObject body1 = new JsonObject()
		body1.addProperty("id", "1")
		body1.addProperty("name", "O'Henry")
		body1.addProperty("email", "ohenry@elastic.io")
		body1.addProperty("birth", "2001-11-11 12:33:33")
		runAction(getPersonsConfig(), body1, snapshot)

		ArrayList<String> records = getRecords("persons")

		expect:
		records.size() == 1
		records.get(0) == '{ID=1, NAME=O\'Henry, BIRTH=2001-11-11 12:33:33.0, EMAIL=ohenry@elastic.io}'
	}

	def "two inserts, one update by email" () {

		preparePersonsTable();
		JsonObject snapshot = new JsonObject()

		JsonObject body1 = new JsonObject()
		body1.addProperty("id", "1")
		body1.addProperty("name", "User1")
		body1.addProperty("email", "user1@elastic.io")
		body1.addProperty("birth", "1231-12-22 12:33:33")
		runAction(getPersonsConfig(), body1, snapshot)

		JsonObject body2 = new JsonObject()
		body2.addProperty("id", "2")
		body2.addProperty("name", "User2")
		body2.addProperty("email", "user2@elastic.io")
		body2.addProperty("birth", "1231-12-22 22:33:33")
		runAction(getPersonsConfig(), body2, snapshot)

		JsonObject body3 = new JsonObject()
		body3.addProperty("id", "3")
		body3.addProperty("name", "User3")
		body3.addProperty("email", "user2@elastic.io")
		body3.addProperty("birth", "1231-12-22 15:33:33")
		runAction(getPersonsConfig(), body3, snapshot)

		ArrayList<String> records = getRecords("persons")

		expect:
		records.size() == 2
		records.get(0) == '{ID=1, NAME=User1, BIRTH=1231-12-22 12:33:33.0, EMAIL=user1@elastic.io}'
		records.get(1) == '{ID=3, NAME=User3, BIRTH=1231-12-22 15:33:33.0, EMAIL=user2@elastic.io}'
	}




}
