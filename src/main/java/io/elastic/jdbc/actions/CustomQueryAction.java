package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomQueryAction implements Module {
  private static final Logger LOGGER = LoggerFactory.getLogger(CustomQueryAction.class);

  @Override
  public void execute(ExecutionParameters parameters) {
    LOGGER.info("Starting execute custom query action");
    final JsonObject configuration = parameters.getConfiguration();
    final JsonObject body = parameters.getMessage().getBody();
    final String dbEngine = Utils.getDbEngine(configuration);
    final String queryString = body.getString("query");
    LOGGER.info("Found dbEngine: '{}' and query: '{}'", dbEngine, queryString);

    JsonArray result = null;
    try (Connection connection = Utils.getConnection(configuration)) {
      connection.setAutoCommit(false);

      ResultSet resultSet = null;
      try (Statement statement = connection.createStatement()) {
        statement.execute(queryString);
        resultSet = statement.getResultSet();
      } catch(Exception e) {
        connection.rollback();
        connection.setAutoCommit(true);
        throw e;
      }

      result = customResultSetToJsonArray(resultSet);
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    LOGGER.trace("Emit data= {}", result);
    parameters.getEventEmitter().emitData(new Message.Builder()
        .body(Json.createObjectBuilder()
            .add("result", result)
            .build()
        ).build());
    LOGGER.info("Custom query action is successfully executed");
  }

  public static JsonArray customResultSetToJsonArray(ResultSet resultSet) throws SQLException{
    JsonArrayBuilder jsonBuilder = Json.createArrayBuilder();

    if (resultSet == null) {
      return jsonBuilder.build();
    }

    ResultSetMetaData metaData = resultSet.getMetaData();

    while(resultSet.next()) {
      JsonObjectBuilder entry = Json.createObjectBuilder();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        Utils.getColumnDataByType(resultSet, metaData, i, entry);
      }
      jsonBuilder.add(entry.build());
    }

    return jsonBuilder.build();
  }
}
