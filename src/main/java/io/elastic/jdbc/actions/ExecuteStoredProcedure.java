package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.QueryBuilders.Query;
import io.elastic.jdbc.QueryFactory;
import io.elastic.jdbc.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;

public class ExecuteStoredProcedure implements Module {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteStoredProcedure.class);

  @Override
  public void execute(ExecutionParameters parameters) {
    final JsonObject body = parameters.getMessage().getBody();
    final JsonObject configuration = parameters.getConfiguration();

    try (Connection connection = Utils.getConnection(configuration)) {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(configuration.getString("dbEngine"));

      JsonObject result = query.callProcedure(connection, body, configuration);

      parameters.getEventEmitter()
          .emitData(new Message.Builder().body(result).build());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
