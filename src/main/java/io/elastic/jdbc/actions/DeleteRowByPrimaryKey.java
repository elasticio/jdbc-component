package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.Engines;
import io.elastic.jdbc.QueryBuilders.Query;
import io.elastic.jdbc.QueryFactory;
import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteRowByPrimaryKey implements Module {

  private static final Logger logger = LoggerFactory.getLogger(LookupRowByPrimaryKey.class);
  private static final String PROPERTY_DB_ENGINE = "dbEngine";
  private static final String PROPERTY_TABLE_NAME = "tableName";
  private static final String PROPERTY_ID_COLUMN = "idColumn";
  private static final String PROPERTY_LOOKUP_VALUE = "lookupValue";
  private static final String PROPERTY_NULLABLE_RESULT = "nullableResult";

  private Connection connection = null;
  private Map<String, String> columnTypes = null;
  public boolean isOracle = false;

  @Override
  public void execute(ExecutionParameters parameters) {
    final JsonObject body = parameters.getMessage().getBody();
    final JsonObject configuration = parameters.getConfiguration();
    JsonObject snapshot = parameters.getSnapshot();
    JsonObjectBuilder row = Json.createObjectBuilder();
    ResultSet rs = null;
    StringBuilder primaryKey = new StringBuilder();
    StringBuilder primaryValue = new StringBuilder();
    Integer primaryKeysCount = 0;
    String tableName = "";
    String dbEngine = "";
    Boolean nullableResult = false;
    Integer rowsCount = 0;

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

    boolean isOracle = dbEngine.equals(Engines.ORACLE.name().toLowerCase());

    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      logger.info("{} = {}", entry.getKey(), entry.getValue());
      primaryKey.append(entry.getKey());
      primaryValue.append(entry.getValue());
      primaryKeysCount++;
    }

    if (primaryKeysCount == 1) {
      logger.info("Executing delete row by primary key action");
      Connection connection = Utils.getConnection(configuration);
      Utils.columnTypes = Utils.getColumnTypes(connection, isOracle, tableName);
      logger.info("Detected column types: " + Utils.columnTypes);
      try {
        QueryFactory queryFactory = new QueryFactory();
        Query query = queryFactory.getQuery(dbEngine);
        logger.info("Lookup parameters: {} = {}", primaryKey.toString(), primaryValue.toString());
        query.from(tableName).lookup(primaryKey.toString(), primaryValue.toString());
        checkConfig(configuration);
        rs = query.executeLookup(connection, body);
        while (rs.next()) {
          rowsCount++;
          if (rowsCount > 1) {
            logger.error("Error: the number of matching rows is not exactly one");
            throw new RuntimeException("Error: the number of matching rows is not exactly one");
          }
        }

        for (Map.Entry<String, JsonValue> entry : configuration.entrySet()) {
          logger.info("Key = " + entry.getKey() + " Value = " + entry.getValue());
        }

        if (rowsCount == 1) {
          int result = query.executeDelete(connection, body);

          if (result == 1) {
            row.add("result", result);
            logger.info("Emitting data");
            parameters.getEventEmitter().emitData(new Message.Builder().body(row.build()).build());
          } else {
            logger.info("Unexpected result");
            throw new RuntimeException("Unexpected result");
          }
        } else if (rowsCount == 0 && nullableResult) {
          row.add("empty dataset", "nothing to delete");
          logger.info("Emitting data");
          parameters.getEventEmitter().emitData(new Message.Builder().body(row.build()).build());
        } else if (rowsCount == 0 && !nullableResult) {
          logger.info("Empty response. Error message will be returned");
          throw new RuntimeException("Empty response");
        }

        snapshot = Json.createObjectBuilder().add(PROPERTY_TABLE_NAME, tableName)
            .add(PROPERTY_ID_COLUMN, primaryKey.toString())
            .add(PROPERTY_LOOKUP_VALUE, primaryValue.toString())
            .add(PROPERTY_NULLABLE_RESULT, nullableResult).build();
        logger.info("Emitting new snapshot {}", snapshot.toString());
        parameters.getEventEmitter().emitSnapshot(snapshot);
      } catch (SQLException e) {
        logger.error("Failed to make request", e.toString());
        throw new RuntimeException(e);
      } finally {
        if (rs != null) {
          try {
            rs.close();
          } catch (SQLException e) {
            logger.error("Failed to close result set", e.toString());
          }
        }
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException e) {
            logger.error("Failed to close connection", e.toString());
          }
        }
      }
    } else {
      logger.error("Error: Should be one Primary Key");
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
