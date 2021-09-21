package io.elastic.jdbc.unit

import io.elastic.jdbc.providers.QueryColumnNamesAndAllowsZeroResultsProvider
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonObjectBuilder

class QueryColumnNamesAndAllowsZeroResultsProviderSpec extends Specification {

  JsonObjectBuilder configuration = Json.createObjectBuilder()
  String sqlQuery
  String wrongSqlQuery

  def setup() {
    sqlQuery = "SELECT * FROM films WHERE watched = @watched:boolean AND created = @created:date AND name = @name"
    wrongSqlQuery = "SELECT * FROM films WHERE watched = @watched.name:boolean"
  }

  def "get metadata model, given json object"() {
    configuration.add("sqlQuery", sqlQuery)
    configuration.add("emitBehaviour", "emitIndividually")

    QueryColumnNamesAndAllowsZeroResultsProvider provider = new QueryColumnNamesAndAllowsZeroResultsProvider()
    JsonObject meta = provider.getMetaModel(configuration.build())
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"watched\":{\"title\":\"watched\",\"type\":\"boolean\"},\"created\":{\"title\":\"created\",\"type\":\"date\"},\"name\":{\"title\":\"name\",\"type\":\"string\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"watched\":{\"title\":\"watched\",\"type\":\"boolean\"},\"created\":{\"title\":\"created\",\"type\":\"date\"},\"name\":{\"title\":\"name\",\"type\":\"string\"}}}}"
  }

  def "get metadata model, wrong sqlQuery"() {
    configuration.add("sqlQuery", wrongSqlQuery)
    given :
    QueryColumnNamesAndAllowsZeroResultsProvider provider = new QueryColumnNamesAndAllowsZeroResultsProvider()
    when:
    provider.getMetaModel(configuration.build())
    then:
    thrown RuntimeException
  }
}