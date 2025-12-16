package io.elastic.jdbc.unit

import io.elastic.jdbc.triggers.GetRowsPollingTrigger
import spock.lang.Specification
import jakarta.json.Json
import jakarta.json.JsonObject
import java.sql.Timestamp

class GetRowsPollingTriggerSpec extends Specification {

    GetRowsPollingTrigger trigger
    Timestamp defaultTimestamp

    def setup() {
        trigger = new GetRowsPollingTrigger()
        // Fixed default time for testing: 2023-01-01 12:00:00
        defaultTimestamp = Timestamp.valueOf("2023-01-01 12:00:00.000")
    }

    def "should return polling value from snapshot when valid"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder()
                .add("pollingValue", "2022-01-01 10:00:00.000")
                .build()
        JsonObject config = Json.createObjectBuilder().build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == Timestamp.valueOf("2022-01-01 10:00:00.000")
    }

    def "should return polling value from config when snapshot is empty"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder().build()
        JsonObject config = Json.createObjectBuilder()
                .add("pollingValue", "2021-01-01 10:00:00.000")
                .build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == Timestamp.valueOf("2021-01-01 10:00:00.000")
    }

    def "should prefer snapshot over config"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder()
                .add("pollingValue", "2022-01-01 10:00:00.000")
                .build()
        JsonObject config = Json.createObjectBuilder()
                .add("pollingValue", "2021-01-01 10:00:00.000")
                .build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == Timestamp.valueOf("2022-01-01 10:00:00.000")
    }

    def "should return default timestamp when values are missing"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder().build()
        JsonObject config = Json.createObjectBuilder().build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == defaultTimestamp
    }

    def "should return default timestamp when values does not match regex (current behavior)"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder().build()
        JsonObject config = Json.createObjectBuilder()
                .add("pollingValue", "invalid-date")
                .build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == defaultTimestamp
    }

    def "should accept date-only format (Bug Fix Case)"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder().build()
        JsonObject config = Json.createObjectBuilder()
                .add("pollingValue", "2020-05-15")
                .build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        // matching the expectation from the issue: should detect date and treat as start of day
        result == Timestamp.valueOf("2020-05-15 00:00:00.000")
    }
}
