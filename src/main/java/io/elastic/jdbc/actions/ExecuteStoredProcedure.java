package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Function;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;

import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;

public class ExecuteStoredProcedure implements Function {

  @Override
  public void execute(ExecutionParameters parameters) {
    final JsonObject body = parameters.getMessage().getBody();
    final JsonObject configuration = parameters.getConfiguration();
    System.out.println("body: "+body.toString());
    System.out.println("configuration: "+configuration.toString());
    try {
      Connection connection = Utils.getConnection(configuration);
      System.out.println("connection: "+connection.toString());
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(configuration.getString("dbEngine"));
      System.out.println("query: "+query.toString());

      JsonObject result = query.callProcedure(connection, body, configuration);
      System.out.println("result: "+result.toString());
      parameters.getEventEmitter()
          .emitData(new Message.Builder().body(result).build());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
