package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.utils.Engines;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertAction implements Module {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertAction.class);

  @Override
  public void execute(ExecutionParameters parameters) {
    LOGGER.info("Starting execute insert action");
    final JsonObject configuration = parameters.getConfiguration();
    final JsonObject body = parameters.getMessage().getBody();
    final String dbEngine = Utils.getDbEngine(configuration);
    final boolean isOracle = dbEngine.equals(Engines.ORACLE.name().toLowerCase());
    final boolean isFirebird = dbEngine.equals(Engines.FIREBIRDSQL.name().toLowerCase());
    final String tableName = Utils.getTableName(configuration, (isOracle || isFirebird));
    LOGGER.info("Found dbEngine: '{}' and tableName: '{}'", dbEngine, tableName);
    try (Connection connection = Utils.getConnection(configuration)) {
      Utils.columnTypes = Utils.getColumnTypes(connection, tableName);
      LOGGER.info("Detected column types: " + Utils.columnTypes);
      LOGGER.info("Inserting in table '{}' values '{}'", tableName, body);
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      query.from(tableName);
      query.executeInsert(connection, tableName, body);
    } catch (SQLException e) {
      if (Utils.reboundIsEnabled(configuration)) {
        List<String> states = Utils.reboundDbState.get(dbEngine);
        if (states.contains(e.getSQLState())) {
          LOGGER.warn("Starting rebound max iter: {}, rebound ttl: {}. Reason: {}", System.getenv("ELASTICIO_REBOUND_LIMIT"), System.getenv("ELASTICIO_REBOUND_INITIAL_EXPIRATION"), e.getMessage());
          parameters.getEventEmitter().emitRebound(e);
          return;
        }
      }
      throw new RuntimeException(e);
    }
    JsonObject result = Json.createObjectBuilder()
        .add("result", true)
        .build();
    LOGGER.info("Emit data= {}", result);
    parameters.getEventEmitter().emitData(new Message.Builder().body(result).build());
    LOGGER.info("Insert action is successfully executed");
  }
}
