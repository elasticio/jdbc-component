package io.elastic.jdbc.unit

import io.elastic.jdbc.Engines
import io.elastic.jdbc.Utils
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
    expectedProperties.setProperty("user", "user")
    expectedProperties.setProperty("password", "password")
    expectedProperties.setProperty("ssl", "true")
    expectedProperties.setProperty("serverTimezone", "UTC")
    Properties properties = Utils.getConfigurationProperties(config, engineType)
    expect:
    properties == expectedProperties
  }
}