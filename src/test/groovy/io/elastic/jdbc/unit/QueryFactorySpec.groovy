package io.elastic.jdbc.unit

import io.elastic.jdbc.query_builders.MSSQL
import io.elastic.jdbc.query_builders.MySQL
import io.elastic.jdbc.query_builders.Oracle
import io.elastic.jdbc.query_builders.PostgreSQL
import io.elastic.jdbc.utils.QueryFactory
import spock.lang.Specification

class QueryFactorySpec extends Specification {

    QueryFactory factory = new QueryFactory()

    def "should return correct Query implementation for known engines"() {
        expect:
        factory.getQuery(engine).getClass() == expectedClass

        where:
        engine       | expectedClass
        "mysql"      | MySQL
        "MYSQL"      | MySQL
        "postgresql" | PostgreSQL
        "PostgreSQL" | PostgreSQL
        "oracle"     | Oracle
        "ORACLE"     | Oracle
        "mssql"      | MSSQL
        "MSSQL"      | MSSQL
    }

    def "should return null for unknown engine"() {
        expect:
        factory.getQuery("unknown") == null
    }

    def "should handle mixed case engines"() {
        expect:
        factory.getQuery("MySql") instanceof MySQL
    }
}
