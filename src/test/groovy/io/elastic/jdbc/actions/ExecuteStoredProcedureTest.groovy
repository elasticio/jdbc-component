import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.ExecuteStoredProcedure
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

@Ignore
class ExecuteStoredProcedureTest extends Specification {
    @Shared
    def user = System.getenv("CONN_USER_ORACLE")
    @Shared
    def password = System.getenv("CONN_PASSWORD_ORACLE")
    @Shared
    def databaseName = System.getenv("CONN_DBNAME_ORACLE")
    @Shared
    def host = System.getenv("CONN_HOST_ORACLE")
    @Shared
    def port = System.getenv("CONN_PORT_ORACLE")
    @Shared
    def connectionString = "jdbc:oracle:thin:@//" + host + ":" + port + "/XE"
    @Shared
    Connection connection

    @Shared
    EventEmitter.Callback errorCallback
    @Shared
    EventEmitter.Callback snapshotCallback
    @Shared
    EventEmitter.Callback dataCallback
    @Shared
    EventEmitter.Callback reboundCallback
    @Shared
    EventEmitter.Callback httpReplyCallback
    @Shared
    EventEmitter emitter
    @Shared
    ExecuteStoredProcedure action

    def setupSpec() {
        connection = DriverManager.getConnection(connectionString, user, password)
    }

    def setup() {
        createAction()
    }

    def createAction() {
        errorCallback = Mock(EventEmitter.Callback)
        snapshotCallback = Mock(EventEmitter.Callback)
        dataCallback = Mock(EventEmitter.Callback)
        reboundCallback = Mock(EventEmitter.Callback)
        httpReplyCallback = Mock(EventEmitter.Callback)
        emitter = new EventEmitter.Builder().onData(dataCallback).onSnapshot(snapshotCallback).onError(errorCallback)
                .onRebound(reboundCallback).onHttpReplyCallback(httpReplyCallback).build()
        action = new ExecuteStoredProcedure()
    }

    def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
        Message msg = new Message.Builder().body(body).build()
        ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
        action.execute(params);
    }

    def getStarsConfig() {
        JsonObject config = Json.createObjectBuilder()
                .add("tableName", "STARS")
                .add("user", user)
                .add("password", password)
                .add("dbEngine", "oracle")
                .add("host", host)
                .add("port", port)
                .add("databaseName", databaseName)
                .add("nullableResult", "true")
                .build();
        return config;
    }

    def prepareStarsTable() {
        String sql = "BEGIN" +
                "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
                "EXCEPTION" +
                "   WHEN OTHERS THEN" +
                "      IF SQLCODE != -942 THEN" +
                "         RAISE;" +
                "      END IF;" +
                "END;"
        connection.createStatement().execute(sql);
        connection.createStatement().execute("CREATE TABLE stars (id number, name varchar(255) NOT NULL, " +
                "radius number, destination float,visible number(1), " +
                "CONSTRAINT pk_stars PRIMARY KEY (id))");
        connection.createStatement().execute("INSERT INTO stars (ID,NAME,RADIUS,DESTINATION, VISIBLE) VALUES (1,'Taurus',321,44.4,1)")
        connection.createStatement().execute("INSERT INTO stars (ID,NAME,RADIUS,DESTINATION, VISIBLE) VALUES (2,'Boston',581,94.4,0)")
    }

    def getRecords(tableName) {
        ArrayList<String> records = new ArrayList<String>();
        String sql = "SELECT * FROM " + tableName;
        ResultSet rs = connection.createStatement().executeQuery(sql);
        while (rs.next()) {
            records.add(rs.toRowResult().toString());
        }
        rs.close();
        return records;
    }

    def cleanupSpec() {
        String sql = "BEGIN" +
                "   EXECUTE IMMEDIATE 'DROP TABLE stars';" +
                "EXCEPTION" +
                "   WHEN OTHERS THEN" +
                "      IF SQLCODE != -942 THEN" +
                "         RAISE;" +
                "      END IF;" +
                "END;"
        connection.createStatement().execute(sql)
        connection.close()
    }

    def "one delete"() {

        prepareStarsTable();

        JsonObject snapshot = Json.createObjectBuilder().build()

        JsonObject body = Json.createObjectBuilder()
                .add("ID", 1)
                .build();

        runAction(getStarsConfig(), body, snapshot)
        int first = getRecords("stars").size()
        JsonObject body2 = Json.createObjectBuilder()
                .add("ID", 2)
                .build()
        runAction(getStarsConfig(), body2, snapshot)
        int second = getRecords("stars").size()

        expect:
        first == 1
        second == 0
    }
}