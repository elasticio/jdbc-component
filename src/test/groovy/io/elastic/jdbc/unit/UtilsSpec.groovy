package io.elastic.jdbc.unit

import io.elastic.jdbc.utils.Engines
import io.elastic.jdbc.utils.Utils
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject

class UtilsSpec extends Specification {

  def "get properties without connection properties"() {
    final Engines engineType = Engines.valueOf("MYSQL");
    JsonObject config = Json.createObjectBuilder()
        .add("user", "user")
        .add("password", "password")
        .build();
    Properties expectedProperties = new Properties();
    expectedProperties.setProperty("connectionTimeZone", "SERVER")
    expectedProperties.setProperty("user", "user")
    expectedProperties.setProperty("password", "password")
    Properties properties = Utils.getConfigurationProperties(config, engineType)
    expect:
    properties == expectedProperties
  }

  def "get properties with single connection property"() {
    final Engines engineType = Engines.valueOf("MYSQL");
    JsonObject config = Json.createObjectBuilder()
        .add("user", "user")
        .add("password", "password")
        .add("configurationProperties", "ssl=true")
        .build();
    Properties expectedProperties = new Properties();
    expectedProperties.setProperty("connectionTimeZone", "SERVER")
    expectedProperties.setProperty("user", "user")
    expectedProperties.setProperty("password", "password")
    expectedProperties.setProperty("ssl", "true")
    Properties properties = Utils.getConfigurationProperties(config, engineType)
    expect:
    properties == expectedProperties
  }

  def "get properties with multiply connection properties"() {
    final Engines engineType = Engines.valueOf("MYSQL");
    JsonObject config = Json.createObjectBuilder()
        .add("user", "user")
        .add("password", "password")
        .add("configurationProperties", "ssl=true&serverTimezone=UTC")
        .build();
    Properties expectedProperties = new Properties();
    expectedProperties.setProperty("connectionTimeZone", "SERVER")
    expectedProperties.setProperty("user", "user")
    expectedProperties.setProperty("password", "password")
    expectedProperties.setProperty("ssl", "true")
    expectedProperties.setProperty("serverTimezone", "UTC")
    Properties properties = Utils.getConfigurationProperties(config, engineType)
    expect:
    properties == expectedProperties
  }

  def "get table name"() {
    JsonObject config = Json.createObjectBuilder()
        .add("tableName", "stars")
        .build();
    JsonObject configOracle = Json.createObjectBuilder()
        .add("tableName", "tableSchema.stars")
        .build();
    String result = Utils.getTableName(config, false)
    String resultOracle = Utils.getTableName(configOracle, true)
    String resultWithPoint = Utils.getTableName(configOracle, false)
    expect:
    result == "stars"
    resultOracle == "STARS"
    resultWithPoint == "stars"
  }

  def "get table name, configuration is empty"() {
    JsonObject config = Json.createObjectBuilder()
        .build()
    when:
    Utils.getTableName(config, false)
    then:
    thrown RuntimeException
  }

  def "get dbEngine"() {
    JsonObject config = Json.createObjectBuilder()
        .add("dbEngine", "oracle")
        .build();
    String result = Utils.getDbEngine(config)
    expect:
    result == "oracle"
  }

  def "get dbEngine, configuration is empty"() {
    JsonObject config = Json.createObjectBuilder()
        .build()
    when:
    Utils.getDbEngine(config)
    then:
    thrown RuntimeException
  }

  def "convert Sql Type"() {
    String resultNumber = Utils.convertType(2)
    String resultBoolean = Utils.convertType(16)
    String resultString = Utils.convertType(111)
    expect:
    resultNumber == "number"
    resultBoolean == "boolean"
    resultString == "string"
  }

  def "get Table Name Pattern"() {
    String result = Utils.getTableNamePattern("stars")
    String resultWithPoint = Utils.getTableNamePattern("tableSchema.stars")
    expect:
    result == "stars"
    resultWithPoint == "stars"
  }

  def "get Schema Name Pattern"() {
    String result = Utils.getSchemaNamePattern("stars")
    String resultWithPoint = Utils.getSchemaNamePattern("tableSchema.stars")
    expect:
    result == null
    resultWithPoint == "tableSchema"
  }
}