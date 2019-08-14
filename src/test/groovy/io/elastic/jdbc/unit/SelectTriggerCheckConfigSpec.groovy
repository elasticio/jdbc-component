package io.elastic.jdbc.unit

import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObjectBuilder
import io.elastic.jdbc.triggers.SelectTrigger

class SelectTriggerCheckConfigSpec extends Specification {
  @Shared
  SelectTrigger trigger

  JsonObjectBuilder configuration = Json.createObjectBuilder()
  String sqlQuery

  def setup() {
    trigger = new SelectTrigger()
  }

  def "check sqlQuery string with @parameter:type"() {
    sqlQuery = 'SELECT * from stars WHERE id= @id:number'
    configuration.add("sqlQuery", sqlQuery)

    when:
    trigger.checkConfig(configuration.build())
    then:
    thrown RuntimeException
  }

  def "check sqlQuery string with @parameter.name:type"() {
    sqlQuery = 'SELECT * from stars WHERE id= @id.name:number'
    configuration.add("sqlQuery", sqlQuery)

    when:
    trigger.checkConfig(configuration.build())
    then:
    thrown RuntimeException
  }

  def "sqlQuery string is empty"() {
    configuration.add("sqlQuery", '')
    when:
    trigger.checkConfig(configuration.build())
    then:
    thrown RuntimeException
  }

  def "confid isn't contains sqlQuery string"() {
    when:
    trigger.checkConfig(configuration.build())
    then:
    thrown RuntimeException
  }

}