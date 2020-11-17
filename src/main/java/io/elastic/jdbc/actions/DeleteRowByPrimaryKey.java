package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Function;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.Engines;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteRowByPrimaryKey implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(LookupRowByPrimaryKey.class);
  private static final String PROPERTY_DB_ENGINE = "dbEngine";
  private static final String PROPERTY_TABLE_NAME = "tableName";
  private static final String PROPERTY_ID_COLUMN = "idColumn";
  private static final String PROPERTY_LOOKUP_VALUE = "lookupValue";
  private static final String PROPERTY_NULLABLE_RESULT = "nullableResult";

  @Override
  public void execute(ExecutionParameters parameters) {
    final JsonObject body = parameters.getMessage().getBody();
    final JsonObject configuration = parameters.getConfiguration();
    JsonObject snapshot = parameters.getSnapshot();
    StringBuilder primaryKey = new StringBuilder();
    StringBuilder primaryValue = new StringBuilder();
    Integer primaryKeysCount = 0;
    String tableName = "";
    String dbEngine = "";
    Boolean nullableResult = false;

    if (configuration.containsKey(PROPERTY_TABLE_NAME)
        && Utils.getNonNullString(configuration, PROPERTY_TABLE_NAME).length() != 0) {
      tableName = configuration.getString(PROPERTY_TABLE_NAME);
    } else if (snapshot.containsKey(PROPERTY_TABLE_NAME)
        && Utils.getNonNullString(snapshot, PROPERTY_TABLE_NAME).length() != 0) {
      tableName = snapshot.getString(PROPERTY_TABLE_NAME);
    } else {
      throw new RuntimeException("Table name is required field");
    }

    if (Utils.getNonNullString(configuration, PROPERTY_DB_ENGINE).length() != 0) {
      dbEngine = configuration.getString(PROPERTY_DB_ENGINE);
    } else if (Utils.getNonNullString(snapshot, PROPERTY_DB_ENGINE).length() != 0) {
      dbEngine = snapshot.getString(PROPERTY_DB_ENGINE);
    } else {
      throw new RuntimeException("DB Engine is required field");
    }

    if (Utils.getNonNullString(configuration, PROPERTY_NULLABLE_RESULT).equals("true")) {
      nullableResult = true;
    } else if (Utils.getNonNullString(snapshot, PROPERTY_NULLABLE_RESULT).equals("true")) {
      nullableResult = true;
    }

    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      primaryKey.append(entry.getKey());
      primaryValue.append(entry.getValue());
      primaryKeysCount++;
    }

    if (primaryKeysCount == 1) {
      try (Connection connection = Utils.getConnection(configuration)) {
        LOGGER.info("Executing delete row by primary key action");
        boolean isOracle = dbEngine.equals(Engines.ORACLE.name().toLowerCase());
        Utils.columnTypes = Utils.getColumnTypes(connection, isOracle, tableName);
        LOGGER.debug("Detected column types");
        try {
          QueryFactory queryFactory = new QueryFactory();
          Query query = queryFactory.getQuery(dbEngine);
          query.from(tableName).lookup(primaryKey.toString(), primaryValue.toString());
          checkConfig(configuration);
          JsonObject row = query.executeLookup(connection, body);

          if (row.size() != 0) {
            int result = query.executeDelete(connection, body);
            if (result == 1) {
              LOGGER.info("Emitting data...");
              parameters.getEventEmitter().emitData(new Message.Builder().body(row).build());
            } else {
              LOGGER.error("Unexpected result");
              throw new RuntimeException("Unexpected result");
            }
          } else if (row.size() == 0 && nullableResult) {
            JsonObject emptyRow = Json.createObjectBuilder()
                .add("empty dataset", "nothing to delete")
                .build();
            LOGGER.info("Emitting data...");
            parameters.getEventEmitter().emitData(new Message.Builder().body(emptyRow).build());
          } else if (row.size() == 0 && !nullableResult) {
            LOGGER.info("Empty response. Error message will be returned");
            throw new RuntimeException("Empty response");
          }

          snapshot = Json.createObjectBuilder().add(PROPERTY_TABLE_NAME, tableName)
              .add(PROPERTY_ID_COLUMN, primaryKey.toString())
              .add(PROPERTY_LOOKUP_VALUE, primaryValue.toString())
              .add(PROPERTY_NULLABLE_RESULT, nullableResult).build();
          LOGGER.info("Emitting new snapshot");
          parameters.getEventEmitter().emitSnapshot(snapshot);
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
          LOGGER.error("Failed to make request");
          throw new RuntimeException(e);
        }
      } catch (SQLException e) {
        LOGGER.error("Failed to close connection");
      }
    } else {
      LOGGER.error("Error: Should be one Primary Key");
      throw new IllegalStateException("Should be one Primary Key");
    }
  }

  private void checkConfig(JsonObject config) {
    final JsonString tableName = config.getJsonString(PROPERTY_TABLE_NAME);

    if (tableName == null || tableName.toString().isEmpty()) {
      throw new RuntimeException("Table name is required");
    }
  }
}
