package io.elastic.jdbc.unit

import io.elastic.jdbc.query_builders.Query
import spock.lang.Specification
import java.sql.Timestamp

class QuerySpec extends Specification {

    // Concrete implementation for testing abstract class
    class TestQuery extends Query {
        @Override
        ArrayList executePolling(java.sql.Connection connection) { return null }
        @Override
        jakarta.json.JsonObject executeLookup(java.sql.Connection connection, jakarta.json.JsonObject body) { return null }
        @Override
        int executeDelete(java.sql.Connection connection, jakarta.json.JsonObject body) { return 0 }
        @Override
        void executeInsert(java.sql.Connection connection, String tableName, jakarta.json.JsonObject body) { }
        @Override
        void executeUpdate(java.sql.Connection connection, String tableName, String idColumn, String idValue, jakarta.json.JsonObject body) { }
        @Override
        java.sql.CallableStatement prepareCallableStatement(java.sql.Connection connection, String procedureName, Map<String, io.elastic.jdbc.utils.ProcedureParameter> procedureParams, jakarta.json.JsonObject messageBody) { return null }
        @Override
        jakarta.json.JsonObject callProcedure(java.sql.Connection connection, jakarta.json.JsonObject body, jakarta.json.JsonObject configuration) { return null }
    }

    TestQuery query = new TestQuery()

    def "should set 'from' correctly"() {
        when:
        query.from("my_table")
        then:
        query.tableName == "my_table"
    }

    def "should set 'orderBy' correctly"() {
        when:
        query.orderBy("id")
        then:
        query.orderField == "id"
    }

    def "should set 'skip' correctly"() {
        when:
        query.skip(10)
        then:
        query.skipNumber == 10
    }

    def "should set 'rowsPolling' correctly"() {
        given:
        def time = new Timestamp(System.currentTimeMillis())
        when:
        query.rowsPolling("created_at", time)
        then:
        query.pollingField == "created_at"
        query.pollingValue == time
    }

    def "should set 'lookup' correctly"() {
        when:
        query.lookup("email", "test@test.com")
        then:
        query.lookupField == "email"
        query.lookupValue == "test@test.com"
    }

    def "should identify SELECT queries"() {
        expect:
        Query.isSelect(sql) == expected

        where:
        sql | expected
        "SELECT * FROM table" | true
        "select * from table" | true
        "  select * "         | false // isSelect does not trim
        "INSERT INTO table"   | false
        "UPDATE table"        | false
    }

    def "should pre-process SELECT queries"() {
        expect:
        Query.preProcessSelect(input) == expected

        where:
        input                                              | expected
        "SELECT * FROM table WHERE id = @id:string"        | "SELECT * FROM table WHERE id = ?"
        "SELECT * FROM T WHERE id=@id AND name=@name:string" | "SELECT * FROM T WHERE id=? AND name=?"
        "  select * from t  "                              | "select * from t" // preProcess trims
    }

    def "should throw exception for non-select queries in preProcessSelect"() {
        when:
        Query.preProcessSelect("DELETE FROM table")
        then:
        thrown(RuntimeException)
    }
}
