package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.utils.Engines;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.json.Json;
import javax.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpsertRowByPrimaryKey implements Module {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertRowByPrimaryKey.class);
  private static final String PROPERTY_DB_ENGINE = "dbEngine";
  private static final String PROPERTY_TABLE_NAME = "tableName";

  @Override
  public void execute(ExecutionParameters parameters) {
    final JsonObject configuration = parameters.getConfiguration();
    final JsonObject body = parameters.getMessage().getBody();
    JsonObject snapshot = parameters.getSnapshot();
    JsonObject resultRow;
    String tableName;
    String dbEngine;
    String catalog = "";
    String schemaName = "";
    String primaryKey = "";
    int primaryKeysCount = 0;

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

    LOGGER.info("Executing lookup primary key");
    final boolean isOracle = dbEngine.equals(Engines.ORACLE.name().toLowerCase());
    final boolean isMysql = dbEngine.equals(Engines.MYSQL.name().toLowerCase());
    final boolean isFirebird = dbEngine.equals(Engines.FIREBIRDSQL.name().toLowerCase());

    try (Connection connection = Utils.getConnection(configuration)) {
      DatabaseMetaData dbMetaData = connection.getMetaData();
      if (isMysql) {
        catalog = configuration.getString("databaseName");
      }
      if (isFirebird) {
        tableName = tableName.toUpperCase();
      }
      if (tableName.contains(".")) {
        schemaName =
            (isOracle) ? tableName.split("\\.")[0].toUpperCase() : tableName.split("\\.")[0];
        tableName =
            (isOracle) ? tableName.split("\\.")[1].toUpperCase() : tableName.split("\\.")[1];
      }
      try (ResultSet rs = dbMetaData
          .getPrimaryKeys(catalog, ((isOracle && !schemaName.isEmpty()) ? schemaName : null),
              tableName)) {
        while (rs.next()) {
          primaryKey = rs.getString("COLUMN_NAME");
          primaryKeysCount++;
        }
        if (primaryKeysCount == 1) {
          LOGGER.info("Executing upsert row by primary key action");
          Utils.columnTypes = Utils.getColumnTypes(connection, tableName);
          LOGGER.info("Detected column types: " + Utils.columnTypes);
          QueryFactory queryFactory = new QueryFactory();
          Query query = queryFactory.getQuery(dbEngine);
          LOGGER
              .info("Execute upsert parameters by PK: '{}' = {}", primaryKey, body.get(primaryKey));
          query.from(tableName);
          resultRow = query.executeUpsert(connection, primaryKey, body);
          LOGGER.info("Emit data= {}", resultRow);
          parameters.getEventEmitter().emitData(new Message.Builder().body(resultRow).build());
          snapshot = Json.createObjectBuilder().add(PROPERTY_TABLE_NAME, tableName).build();
          LOGGER.info("Emitting new snapshot {}", snapshot.toString());
          parameters.getEventEmitter().emitSnapshot(snapshot);
        } else if (primaryKeysCount == 0) {
          LOGGER.error("Error: Table has not Primary Key. Should be one Primary Key");
          throw new IllegalStateException("Table has not Primary Key. Should be one Primary Key");
        } else {
          LOGGER.error("Error: Composite Primary Key is not supported");
          throw new IllegalStateException("Composite Primary Key is not supported");
        }
      }
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
  }
}
