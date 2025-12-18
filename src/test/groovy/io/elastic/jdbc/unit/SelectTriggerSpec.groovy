package io.elastic.jdbc.unit

import io.elastic.jdbc.triggers.SelectTrigger
import spock.lang.Specification
import jakarta.json.Json
import jakarta.json.JsonObject
import java.sql.Timestamp

class SelectTriggerSpec extends Specification {

    SelectTrigger trigger
    Timestamp defaultTimestamp

    def setup() {
        trigger = new SelectTrigger()
        // Fixed default time for testing: 2023-01-01 12:00:00
        defaultTimestamp = Timestamp.valueOf("2023-01-01 12:00:00.000")
    }

    def "should return polling value from snapshot (placeholder key) when valid"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder()
                .add("%%EIO_LAST_POLL%%", "2022-01-01 10:00:00.000")
                .build()
        JsonObject config = Json.createObjectBuilder().build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == Timestamp.valueOf("2022-01-01 10:00:00.000")
    }

    def "should return polling value from snapshot (pollingValue key) when valid"() {
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
                .add("%%EIO_LAST_POLL%%", "2022-01-01 10:00:00.000")
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

    def "should accept date-only format"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder().build()
        JsonObject config = Json.createObjectBuilder()
                .add("pollingValue", "2020-05-15")
                .build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == Timestamp.valueOf("2020-05-15 00:00:00.000")
    }

    def "should handle leading and trailing whitespace"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder()
                .add("%%EIO_LAST_POLL%%", "  2022-01-01 10:00:00.000  ")
                .build()
        JsonObject config = Json.createObjectBuilder().build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == Timestamp.valueOf("2022-01-01 10:00:00.000")
    }

    def "should accept high-precision timestamps"() {
        given:
        JsonObject snapshot = Json.createObjectBuilder()
                .add("%%EIO_LAST_POLL%%", "2023-10-27 10:20:30.123456789")
                .build()
        JsonObject config = Json.createObjectBuilder().build()

        when:
        def result = trigger.getPollingValue(config, snapshot, defaultTimestamp)

        then:
        result == Timestamp.valueOf("2023-10-27 10:20:30.123456789")
    }

    // Since SelectTrigger.execute is hard to test without mocking Connection/Query, 
    // I will verify the logic if I can, but for now I've verified getPollingValue.
}
