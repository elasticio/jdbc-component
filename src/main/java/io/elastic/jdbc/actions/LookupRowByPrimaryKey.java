package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.JSON;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.utils.Engines;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupRowByPrimaryKey implements Module {

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
    int primaryKeysCount = 0;
    String tableName = "";
    String dbEngine = "";
    boolean nullableResult = false;

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
      LOGGER.info("{} = {}", entry.getKey(), entry.getValue());
      primaryKey.append(entry.getKey());
      primaryValue.append(entry.getValue());
      primaryKeysCount++;
    }

    if (primaryKeysCount == 1) {

      try (Connection connection = Utils.getConnection(configuration)) {
        LOGGER.info("Executing lookup row by primary key action");
        Utils.columnTypes = Utils.getColumnTypes(connection, tableName);
        LOGGER.info("Detected column types: " + Utils.columnTypes);
        try {
          QueryFactory queryFactory = new QueryFactory();
          Query query = queryFactory.getQuery(dbEngine);
          LOGGER.info("Lookup parameters: {} = {}", primaryKey.toString(), primaryValue.toString());
          query.from(tableName).lookup(primaryKey.toString(), primaryValue.toString());
          checkConfig(configuration);

          JsonObject row = query.executeLookup(connection, body);
          if (row.size() != 0) {
            LOGGER.info("Emitting data");
            LOGGER.info(row.toString());
            parameters.getEventEmitter().emitData(new Message.Builder().body(row).build());
          }
          if (row.size() == 0 && nullableResult) {
            JsonObjectBuilder emptyResBuilder = Json.createObjectBuilder();
            emptyResBuilder.add("empty dataset", JsonValue.NULL);
            LOGGER.info("Emitting data");
            LOGGER.info(JSON.stringify(emptyResBuilder.build()));
            parameters.getEventEmitter().emitData(new Message.Builder().body(emptyResBuilder.build()).build());
          } else if (row.size() == 0 && !nullableResult) {
            LOGGER.info("Empty response. Error message will be returned");
            throw new RuntimeException("Empty response");
          }

          snapshot = Json.createObjectBuilder().add(PROPERTY_TABLE_NAME, tableName)
              .add(PROPERTY_ID_COLUMN, primaryKey.toString())
              .add(PROPERTY_LOOKUP_VALUE, primaryValue.toString())
              .add(PROPERTY_NULLABLE_RESULT, nullableResult).build();
          LOGGER.info("Emitting new snapshot {}", snapshot.toString());
          parameters.getEventEmitter().emitSnapshot(snapshot);
        } catch (SQLException e) {
          LOGGER.error("Failed to make request", e.toString());
          throw new RuntimeException(e);
        }
      } catch (SQLException e) {
        LOGGER.error("Failed to close connection", e.toString());
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
