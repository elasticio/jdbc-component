package io.elastic.jdbc.unit

import io.elastic.jdbc.providers.QueryColumnNamesProvider
import spock.lang.*

import jakarta.json.Json
import jakarta.json.JsonObject
import jakarta.json.JsonObjectBuilder

class QueryColumnNamesProviderSpec extends Specification {

  JsonObjectBuilder configuration = Json.createObjectBuilder()
  String sqlQuery
  String wrongSqlQuery

  def setup() {
    sqlQuery = "SELECT * FROM films WHERE watched = @watched:boolean AND created = @created:date AND name = @name"
    wrongSqlQuery = "SELECT * FROM films WHERE watched = @watched.name:boolean"
  }

  def "get metadata model, given json object"() {
    configuration.add("sqlQuery", sqlQuery)

    QueryColumnNamesProvider provider = new QueryColumnNamesProvider()
    JsonObject meta = provider.getMetaModel(configuration.build())
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"watched\":{\"title\":\"watched\",\"type\":\"boolean\"},\"created\":{\"title\":\"created\",\"type\":\"date\"},\"name\":{\"title\":\"name\",\"type\":\"string\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"watched\":{\"title\":\"watched\",\"type\":\"boolean\"},\"created\":{\"title\":\"created\",\"type\":\"date\"},\"name\":{\"title\":\"name\",\"type\":\"string\"}}}}"
  }

  def "get metadata model, wrong sqlQuery"() {
    configuration.add("sqlQuery", wrongSqlQuery)
    given :
    QueryColumnNamesProvider provider = new QueryColumnNamesProvider()
    when:
    provider.getMetaModel(configuration.build())
    then:
    thrown RuntimeException
  }
}