package io.elastic.jdbc.integration.triggers.get_rows_polling_trigger

import io.elastic.api.EventEmitter
import io.elastic.api.EventEmitter.Callback
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.TestUtils
import io.elastic.jdbc.triggers.GetRowsPollingTrigger
import spock.lang.*

import jakarta.json.Json
import jakarta.json.JsonObject
import jakarta.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

class GetRowsPollingTriggerMySQLHiredDateSpec extends Specification {

    @Shared
    Connection connection

    def setup() {
        JsonObject config = TestUtils.getMysqlConfigurationBuilder().build()
        connection = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))

        String sql = "DROP TABLE IF EXISTS employees_hired_date"
        connection.createStatement().execute(sql)

        sql = "CREATE TABLE employees_hired_date (id INT, name VARCHAR(255), hire_date DATE)"
        connection.createStatement().execute(sql)

        sql = "INSERT INTO employees_hired_date (id, name, hire_date) VALUES (1, 'John', '2023-01-10')"
        connection.createStatement().execute(sql)
        sql = "INSERT INTO employees_hired_date (id, name, hire_date) VALUES (2, 'Jane', '2023-01-15')"
        connection.createStatement().execute(sql)
        sql = "INSERT INTO employees_hired_date (id, name, hire_date) VALUES (3, 'Mike', '2023-01-20')"
        connection.createStatement().execute(sql)
    }

    def cleanupSpec() {
        connection.createStatement().execute("DROP TABLE employees_hired_date")
        connection.close()
    }

    def "make a SELECT request with hire_date"() {

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
            .onRebound(onreboundCallback).build()

        GetRowsPollingTrigger getRowsPollingTrigger = new GetRowsPollingTrigger()

        given:
        Message msg = new Message.Builder().build()

        JsonObjectBuilder config = TestUtils.getMysqlConfigurationBuilder()
        config.add("pollingField", "hire_date")
            .add("pollingValue", "2023-01-15")
            .add("tableName", "employees_hired_date")

        JsonObjectBuilder snapshot = Json.createObjectBuilder()

        when:
        ExecutionParameters params = new ExecutionParameters(msg, emitter, config.build(), snapshot.build())
        getRowsPollingTrigger.execute(params)

        then:
        1 * dataCallback.receive({
            assert it.body.getInt("id") == 3
            assert it.body.getString("name") == "Mike"
            assert it.body.getString("hire_date") == "2023-01-20"
        })
        0 * errorCallback.receive(_)
    }
}
