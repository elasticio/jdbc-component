package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.QueryBuilders.Query;
import io.elastic.jdbc.QueryFactory;
import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectAction implements Module {

  private static final Logger logger = LoggerFactory.getLogger(SelectAction.class);
  private static final String SQL_QUERY_VALUE = "sqlQuery";
  private static final String PROPERTY_NULLABLE_RESULT = "nullableResult";
  private static final String PROPERTY_SKIP_NUMBER = "skipNumber";

  @Override
  public void execute(ExecutionParameters parameters) {
    final JsonObject body = parameters.getMessage().getBody();
    final JsonObject configuration = parameters.getConfiguration();
    JsonObject snapshot = parameters.getSnapshot();
    JsonObjectBuilder row = Json.createObjectBuilder();
    checkConfig(configuration);
    Connection connection = Utils.getConnection(configuration);
    String dbEngine = configuration.getString("dbEngine");
    String sqlQuery = configuration.getString("sqlQuery");
    Integer skipNumber = 0;
    Boolean nullableResult = false;
    Integer rowsCount = 0;

    if (Utils.getNonNullString(configuration, PROPERTY_NULLABLE_RESULT).equals("true")) {
      nullableResult = true;
    } else if (Utils.getNonNullString(snapshot, PROPERTY_NULLABLE_RESULT).equals("true")) {
      nullableResult = true;
    }

    if (snapshot.get(PROPERTY_SKIP_NUMBER) != null) {
      skipNumber = snapshot.getInt(PROPERTY_SKIP_NUMBER);
    }

    Utils.columnTypes = Utils.getVariableTypes(sqlQuery);
    logger.info("Detected column types: " + Utils.columnTypes);
    ResultSet rs = null;
    logger.info("Executing select trigger");
    try {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      sqlQuery = Query.preProcessSelect(sqlQuery);
      logger.info("SQL Query: {}", sqlQuery);
      rs = query.executeSelectQuery(connection, sqlQuery, body);
      ResultSetMetaData metaData = rs.getMetaData();
      while (rs.next()) {
        logger.info("columns count: {} from {}", rowsCount, metaData.getColumnCount());
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          row = Utils.getColumnDataByType(rs, metaData, i, row);
        }
        rowsCount++;
        logger.info("Emitting data");
        logger.info(row.toString());
        parameters.getEventEmitter().emitData(new Message.Builder().body(row.build()).build());
      }

      if (rowsCount == 0 && nullableResult) {
        row.add("empty dataset", "no data");
        logger.info("Emitting data");
        parameters.getEventEmitter().emitData(new Message.Builder().body(row.build()).build());
      } else if (rowsCount == 0 && !nullableResult) {
        logger.info("Empty response. Error message will be returned");
        throw new RuntimeException("Empty response");
      }

      snapshot = Json.createObjectBuilder().add(PROPERTY_SKIP_NUMBER, skipNumber + rowsCount)
          .add(SQL_QUERY_VALUE, sqlQuery)
          .add(PROPERTY_NULLABLE_RESULT, nullableResult).build();
      logger.info("Emitting new snapshot {}", snapshot.toString());
      parameters.getEventEmitter().emitSnapshot(snapshot);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error(e.toString());
        }
      }
    }
  }

  private void checkConfig(JsonObject config) {
    final JsonString sqlQuery = config.getJsonString(SQL_QUERY_VALUE);

    if (sqlQuery == null || sqlQuery.toString().isEmpty()) {
      throw new RuntimeException("SQL Query is required field");
    }
  }
}
