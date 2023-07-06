package io.elastic.jdbc.integration

import io.elastic.jdbc.JdbcCredentialsVerifier
import io.elastic.jdbc.TestUtils
import spock.lang.Specification

import jakarta.json.Json
import jakarta.json.JsonObject
import io.elastic.api.InvalidCredentialsException

class JdbcCredentialsVerifierSpec extends Specification {

  def "should verify successfully when connection succeeds"() {
    setup:
    JsonObject mssqlConfig = TestUtils.getMssqlConfigurationBuilder()
        .add("configurationProperties", "encrypt=false&integratedSecurity=false")
        .build()
    when:
    new JdbcCredentialsVerifier().verify(mssqlConfig)

    then:
    notThrown(Throwable.class)
  }

  def "should verify successfully MySql"() {
    setup:
    JsonObject mysqlConfig = TestUtils.getMysqlConfigurationBuilder()
        .add("configurationProperties", "serverTimezone=UTC&ssl=true")
        .build()
    when:
    new JdbcCredentialsVerifier().verify(mysqlConfig)

    then:
    notThrown(Throwable.class)
  }

  def "should verify successfully Postgresql"() {
    setup:
    JsonObject postgresqlConfig = TestUtils.getPostgresqlConfigurationBuilder()
        .add("configurationProperties", "readOnly=true&logUnclosedConnections=true")
        .build()
    when:
    new JdbcCredentialsVerifier().verify(postgresqlConfig)

    then:
    notThrown(Throwable.class)
  }

  def "should verify successfully Oracle"() {
    setup:
    JsonObject oracleConfig = TestUtils.getOracleConfigurationBuilder()
        .add("configurationProperties", "CatalogOptions=0&ConnectionRetryCount=3")
        .build()
    when:
    new JdbcCredentialsVerifier().verify(oracleConfig)

    then:
    notThrown(Throwable.class)
  }

  def "should not verify when connection fails"() {
    setup:
    JsonObject config = Json.createObjectBuilder()
        .add("dbEngine", "mysql")
        .add("host", "localhost")
        .add("port", "3306")
        .add("databaseName", "testdb")
        .add("user", "admin")
        .add("password", "secret")
        .build()

    when:
    new JdbcCredentialsVerifier().verify(config)

    then:
    def e = thrown(InvalidCredentialsException.class)
    e.message == "Failed to connect to database"
  }
}
