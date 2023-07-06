package io.elastic.jdbc.integration.actions.insert_action

import io.elastic.jdbc.TestUtils
import org.junit.Test
import spock.lang.Shared
import spock.lang.Specification

import jakarta.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.CountDownLatch

class DeadlockSpec extends Specification {
    @Shared
    String deadlockTable1 = "deadlock_test1"
    @Shared
    String deadlockTable2 = "deadlock_test2"
    @Shared
    JsonObject mySQLConfig
    @Shared
    JsonObject postgresConfig
    @Shared
    JsonObject msSQLConfig
    @Shared
    JsonObject oracleConfig

    def setupSpec() {
        mySQLConfig = TestUtils.getMysqlConfigurationBuilder()
                .build()
        postgresConfig = TestUtils.getPostgresqlConfigurationBuilder()
                .build()
        msSQLConfig = TestUtils.getMssqlConfigurationBuilder()
                .build()
        oracleConfig = TestUtils.getOracleConfigurationBuilder()
                .build()
    }

    def createTestTables(JsonObject config) {
        Connection conn
        try {
            conn = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
            Statement statement = conn.createStatement();
            statement.execute("CREATE TABLE " + deadlockTable1 + " (FOO INTEGER)")
            statement.execute("CREATE TABLE " + deadlockTable2 + " (FOO INTEGER)")
            statement.executeUpdate("INSERT INTO " + deadlockTable1 + " VALUES (0)")
            statement.executeUpdate("INSERT INTO " + deadlockTable2 + " VALUES (0)")
        } finally {
            if (conn) {
                conn.close()
            }
        }
    }

    def cleanTables(JsonObject config) {
        Connection conn
        try {
            conn = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
            Statement statement = conn.createStatement();
            statement.execute("DROP TABLE " + deadlockTable1)
            statement.execute("DROP TABLE " + deadlockTable2)
        } catch (Exception e) {
            println "Failed to cleanup tables"
            println e
        } finally {
            if (conn) {
                conn.close()
            }
        }
    }

    @Test
    def "deadlock MySQL"() {
        boolean isDeadlocked = false
        try {
            createTestTables(mySQLConfig)
            isDeadlocked = deadlockDB(mySQLConfig)
            // STATE: 40001, XA102
            // CODE: 1213, 1614
        } finally {
            cleanTables(mySQLConfig)
        }
        expect:
        isDeadlocked
    }

    def "deadlock MsSQL"() {
        boolean isDeadlocked = false
        try {
            createTestTables(msSQLConfig)
            isDeadlocked = deadlockDB(msSQLConfig)
            // STATE: 40001
            // CODE:1205,3635,5231,5252,17888,23424
        } finally {
            cleanTables(msSQLConfig)
        }
        expect:
        isDeadlocked
    }


    def "deadlock PostgreSQL"() {
        boolean isDeadlocked = false
        try {
            createTestTables(postgresConfig)
            isDeadlocked = deadlockDB(postgresConfig)
            // STATE: 40P01
            // CODE: 0
        } finally {
            cleanTables(postgresConfig)
        }
        expect:
        isDeadlocked
    }

    def "deadlock Oracle"() {
        boolean isDeadlocked = false
        try {
            createTestTables(oracleConfig)
            isDeadlocked = deadlockDB(oracleConfig)
            // STATE: 61000
            // CODE: 60
        } finally {
            cleanTables(oracleConfig)
        }
        expect:
        isDeadlocked
    }

    def deadlockDB(JsonObject config) {
        CountDownLatch latch = new CountDownLatch(2);
        boolean result = false
        Thread t1 = new Thread({ ->
            Connection conn
            try {
                conn = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
                conn.setAutoCommit(false)
                Statement statement = conn.createStatement()
                statement.executeUpdate("UPDATE " + deadlockTable1 + " SET FOO=1")
                latch.countDown()
                latch.await()
                statement.executeUpdate("UPDATE " + deadlockTable2 + " SET FOO=1")
                conn.commit()
            } catch (SQLException e) {
                println e
                println "STATE: " + e.getSQLState()
                println "CODE: " + e.getErrorCode()
                result = true
            } finally {
                if (conn) {
                    conn.close()
                }
            }
        }, "A-THEN-B");
        Thread t2 = new Thread({ ->
            Connection conn
            try {
                conn = DriverManager.getConnection(config.getString("connectionString"), config.getString("user"), config.getString("password"))
                conn.setAutoCommit(false)
                Statement statement = conn.createStatement()
                statement.executeUpdate("UPDATE " + deadlockTable2 + " SET FOO=1")
                latch.countDown()
                latch.await()
                statement.executeUpdate("UPDATE " + deadlockTable1 + " SET FOO=1")
                conn.commit()
            } catch (SQLException e) {
                println e
                println "STATE: " + e.getSQLState()
                println "CODE: " + e.getErrorCode()
                result = true
            } finally {
                if (conn) {
                    conn.close()
                }
            }
        }, "B-THEN-A")
        if (Math.random() < 0.5) {
            t1.start()
            t2.start()
        } else {
            t2.start()
            t1.start()
        }
        t1.join()
        t2.join()
        return result
    }
}
