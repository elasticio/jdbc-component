package io.elastic.jdbc.actions;

import io.elastic.api.EventEmitter;
import io.elastic.api.ExecutionParameters;
import io.elastic.api.Function;
import io.elastic.api.Message;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NewSelectAction implements Function {
  private static final Logger LOGGER = LoggerFactory.getLogger(NewSelectAction.class);
  private static final String SQL_QUERY_VALUE = "sqlQuery";
  private static final String PROPERTY_ALLOW_ZERO_RESULT = "allowZeroResults";
  private static final String EMIT_BEHAVIOUR = "emitBehaviour";

  @Override
  public void execute(ExecutionParameters parameters) {
    JsonObject body = parameters.getMessage().getBody();
    final JsonObject configuration = parameters.getConfiguration();
    EventEmitter eventEmitter = parameters.getEventEmitter();
    checkConfig(configuration);
    String dbEngine = configuration.getString("dbEngine");
    String sqlQuery = configuration.getString("sqlQuery");
    String emitBehaviour = "emitIndividually";

    try {
      emitBehaviour = configuration.getString(EMIT_BEHAVIOUR);
    } catch (NullPointerException e) {
      LOGGER.info("No Emit behavior is specified, the default value Emit Individually will be used");
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

      JsonObject queryBody = removeProperty(body,PROPERTY_ALLOW_ZERO_RESULT);
      resultList = query.executeSelectQuery(connection, sqlQuery, queryBody);
      switch (emitBehaviour) {
        case "fetchAll":
          emitAllData(resultList, eventEmitter);
          break;
        case "emitIndividually":
          emitIndividuallyData(resultList, eventEmitter);
          break;
        case "expectSingle":
          emitSingleData(resultList, eventEmitter, body);
          break;
      }
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

  public static JsonObject removeProperty(JsonObject origin, String key){
    JsonObjectBuilder builder = Json.createObjectBuilder();

    for (Map.Entry<String, JsonValue> entry : origin.entrySet()){
      if (!entry.getKey().equals(key)){
        builder.add(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private void emitIndividuallyData(ArrayList<JsonObject> resultList, EventEmitter eventEmitter) {
    LOGGER.info("Emit Individually option detected");
    for (int i = 0; i < resultList.size(); i++) {
      LOGGER.debug("Columns count: {} from {}", i + 1, resultList.size());
      LOGGER.info("Emitting data...");
      eventEmitter.emitData(new Message.Builder().body(resultList.get(i)).build());
    }
  }

  private void emitAllData(ArrayList<JsonObject> resultList, EventEmitter eventEmitter) {
    LOGGER.info("Fetch All option detected");
    JsonArrayBuilder resultsArray = Json.createArrayBuilder();
    LOGGER.debug("Going to emit result array with size {}", resultList.size());
    for (JsonObject jsonObject : resultList) {
      resultsArray.add(jsonObject);
    }
    JsonObject results = Json.createObjectBuilder()
        .add("results", resultsArray.build())
        .build();
    LOGGER.info("Emitting data...");
    eventEmitter.emitData(new Message.Builder().body(results).build());
  }

  private void emitSingleData(ArrayList<JsonObject> resultList, EventEmitter eventEmitter, JsonObject body) {
    LOGGER.info("Expect Single option detected");
    String allowZeroResults = Utils.getNonNullString(body, PROPERTY_ALLOW_ZERO_RESULT);
    boolean isAllowZeroResults;
    switch (allowZeroResults){
      case "true":
        isAllowZeroResults = true;
        break;
      case "false":
      case "":
        isAllowZeroResults = false;
        break;
      default:
        throw new RuntimeException("Incorrect value for 'Allow Zero Results' property. Expected boolean value: true or false");
    }
    if (resultList.size() == 1){
      LOGGER.info("Emitting data...");
      eventEmitter.emitData(new Message.Builder().body(resultList.get(0)).build());
    } else if (resultList.size() == 0 && isAllowZeroResults){
      LOGGER.info("No data was found but `Allow Zero Results` option activated, emitting an empty object...");
      JsonObjectBuilder emptyResult = Json.createObjectBuilder();
      eventEmitter.emitData(new Message.Builder().body(emptyResult.build()).build());
    } else {
      throw new RuntimeException("Incorrect Number of Results Found. Expected single result");
    }
  }
}
