package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Function;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomQuery implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomQuery.class);

  private static final String JSON_RESULT_ARRAY_NAME = "result";
  private static final String JSON_RESULT_COUNT_NAME = "updated";

  @Override
  public void execute(ExecutionParameters parameters) {
    LOGGER.info("Starting execute custom query action");
    final JsonObject configuration = parameters.getConfiguration();
    final JsonObject body = parameters.getMessage().getBody();
    final String dbEngine = Utils.getDbEngine(configuration);
    final String queryString = body.getString("query");
    LOGGER.info("Found dbEngine: '{}'", dbEngine);

    List<Message> messages = new ArrayList<>();
    try {
      Connection connection = Utils.getConnection(configuration);
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        boolean status = statement.execute(queryString);
        if (status) {
          ResultSet resultSet = statement.getResultSet();
          messages.add(this.processResultSetToMessage(resultSet));
        } else {
          messages.add(this.processUpdateCountToMessage(statement.getUpdateCount()));
        }

        while (statement.getMoreResults() || statement.getUpdateCount() != -1) {
          if (statement.getUpdateCount() != -1) {
            messages.add(this.processUpdateCountToMessage(statement.getUpdateCount()));
          } else {
            messages.add(this.processResultSetToMessage(statement.getResultSet()));
          }
        }

        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        connection.setAutoCommit(true);
        throw e;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    messages.forEach(message -> {
      LOGGER.trace("Emitting message data");
      parameters.getEventEmitter().emitData(message);
    });

    LOGGER.info("Custom query action is successfully executed");
  }

  private Message processResultSetToMessage(ResultSet resultSet) throws SQLException {
    JsonArray result = customResultSetToJsonArray(resultSet);
    return new Message.Builder()
        .body(Json.createObjectBuilder()
            .add(JSON_RESULT_ARRAY_NAME, result)
            .build()
        ).build();
  }

  private Message processUpdateCountToMessage(int updateCount) {
    return new Message.Builder()
        .body(Json.createObjectBuilder()
            .add(JSON_RESULT_COUNT_NAME, updateCount)
            .build()
        ).build();
  }

  public static JsonArray customResultSetToJsonArray(ResultSet resultSet) throws SQLException {
    JsonArrayBuilder jsonBuilder = Json.createArrayBuilder();

    if (resultSet == null) {
      return jsonBuilder.build();
    }

    ResultSetMetaData metaData = resultSet.getMetaData();

    while (resultSet.next()) {
      JsonObjectBuilder entry = Json.createObjectBuilder();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        Utils.getColumnDataByType(resultSet, metaData, i, entry);
      }
      jsonBuilder.add(entry.build());
    }

    return jsonBuilder.build();
  }
}
