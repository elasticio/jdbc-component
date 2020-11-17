package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Function;
import io.elastic.api.Message;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.Engines;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class InsertAction implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertAction.class);

  @Override
  public void execute(ExecutionParameters parameters) {
    LOGGER.info("Starting execute insert action");
    final JsonObject configuration = parameters.getConfiguration();
    final JsonObject body = parameters.getMessage().getBody();
    final String dbEngine = Utils.getDbEngine(configuration);
    final boolean isOracle = dbEngine.equals(Engines.ORACLE.name().toLowerCase());
    final String tableName = Utils.getTableName(configuration, isOracle);
    LOGGER.info("Found dbEngine: '{}'", dbEngine);
    try (Connection connection = Utils.getConnection(configuration)) {
      Utils.columnTypes = Utils.getColumnTypes(connection, isOracle, tableName);
      LOGGER.debug("Detected column types");
      LOGGER.info("Inserting values in the table");
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      query.from(tableName);
      query.executeInsert(connection, tableName, body);
    } catch (SQLException e) {
      if (Utils.reboundIsEnabled(configuration)) {
        List<String> states = Utils.reboundDbState.get(dbEngine);
        if (states.contains(e.getSQLState())) {
          LOGGER.warn("Starting rebound max iter: {}, rebound ttl: {} because of a SQL Exception",
              System.getenv("ELASTICIO_REBOUND_LIMIT"),
              System.getenv("ELASTICIO_REBOUND_INITIAL_EXPIRATION"));
          parameters.getEventEmitter().emitRebound(e);
          return;
        }
      }
      throw new RuntimeException(e);
    }
    JsonObject result = Json.createObjectBuilder()
        .add("result", true)
        .build();
    LOGGER.info("Emit data...");
    parameters.getEventEmitter().emitData(new Message.Builder().body(result).build());
    LOGGER.info("Insert action is successfully executed");
  }
}
