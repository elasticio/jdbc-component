package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Function;
import io.elastic.api.ShutdownParameters;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class SelectAction implements Function {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectAction.class);
  private static final String SQL_QUERY_VALUE = "sqlQuery";
  private static final String PROPERTY_NULLABLE_RESULT = "nullableResult";
  private static final String PROPERTY_SKIP_NUMBER = "skipNumber";

  @Override
  public void execute(ExecutionParameters parameters) {
    final JsonObject body = parameters.getMessage().getBody();
    final JsonObject configuration = parameters.getConfiguration();
    JsonObject snapshot = parameters.getSnapshot();
    checkConfig(configuration);
    String dbEngine = configuration.getString("dbEngine");
    String sqlQuery = configuration.getString("sqlQuery");
    Integer skipNumber = 0;
    Boolean nullableResult = false;

    if (Utils.getNonNullString(configuration, PROPERTY_NULLABLE_RESULT).equals("true")) {
      nullableResult = true;
    } else if (Utils.getNonNullString(snapshot, PROPERTY_NULLABLE_RESULT).equals("true")) {
      nullableResult = true;
    }

    if (snapshot.get(PROPERTY_SKIP_NUMBER) != null) {
      skipNumber = snapshot.getInt(PROPERTY_SKIP_NUMBER);
    }

    Utils.columnTypes = Utils.getVariableTypes(sqlQuery);
    LOGGER.info("Executing select action");
    LOGGER.debug("Detected column types");
    try {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      sqlQuery = Query.preProcessSelect(sqlQuery);
      LOGGER.debug("Got SQL Query");
      ArrayList<JsonObject> resultList;
      Connection connection = Utils.getConnection(configuration);
      resultList = query.executeSelectQuery(connection, sqlQuery, body);
      for (int i = 0; i < resultList.size(); i++) {
        LOGGER.debug("Columns count: {} from {}", i + 1, resultList.size());
        LOGGER.info("Emitting data...");
        parameters.getEventEmitter()
            .emitData(new Message.Builder().body(resultList.get(i)).build());
      }

      if (resultList.size() == 0 && nullableResult) {
        resultList.add(Json.createObjectBuilder()
            .add("empty dataset", "no data")
            .build());
        LOGGER.info("Emitting data...");
        parameters.getEventEmitter()
            .emitData(new Message.Builder().body(resultList.get(0)).build());
      } else if (resultList.size() == 0 && !nullableResult) {
        LOGGER.info("Empty response. Error message will be returned");
        throw new RuntimeException("Empty response");
      }

      snapshot = Json.createObjectBuilder()
          .add(PROPERTY_SKIP_NUMBER, skipNumber + resultList.size())
          .add(SQL_QUERY_VALUE, sqlQuery)
          .add(PROPERTY_NULLABLE_RESULT, nullableResult).build();
      LOGGER.info("Emitting new snapshot");
      parameters.getEventEmitter().emitSnapshot(snapshot);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkConfig(JsonObject config) {
    final JsonString sqlQuery = config.getJsonString(SQL_QUERY_VALUE);

    if (sqlQuery == null || sqlQuery.toString().isEmpty()) {
      throw new RuntimeException("SQL Query is required field");
    }
  }
}
